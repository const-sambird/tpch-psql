import logging
from multiprocessing import Process, Queue
from connection import Connection
from replica import Replica
from query_stream import QueryStream

from tpc_const import QS_ORDER

class Benchmark:
    def __init__(self, queries: list[str], rf1_data, rf2_data, replicas: list[Replica], routes: list[int], indexes: list[list[str, list[str]]], n_query_streams: int, scale_factor: int):
        self.queries = queries
        self.replicas = replicas
        self.routes = routes
        self.indexes = indexes
        self.n_query_streams = n_query_streams
        self.scale_factor = scale_factor
        self.timer_queue = Queue()

        self.power_stream = QueryStream(0, replicas, queries, routes, QS_ORDER[0], rf1_data[0], rf2_data[0], self.timer_queue)
        self.refresh_stream = QueryStream(n_query_streams + 1, replicas, queries, routes, [], rf1_data[-1], rf2_data[-1], self.timer_queue)
        self.throughput_streams = []

        for i in range(1, n_query_streams + 1):
            self.throughput_streams.append(QueryStream(i, replicas, queries, routes, QS_ORDER[i % 100], rf1_data, rf2_data, self.timer_queue))

        #self._create_indexes()
    
    def _create_indexes(self):
        logging.info('creating indexes!')
        connections = [Connection(replica) for replica in self.replicas]
        cursors = [c.conn().cursor() for c in connections]

        indexes_created = 0

        for i_rep, config in enumerate(self.indexes):
            cur = cursors[i_rep]
            for index in config:
                indexes_created += 1
                cur.execute(f'CREATE INDEX idx_{indexes_created} ON {index[0]} ({','.join(index[1])})')
        
        for cur in cursors:
            cur.close()
        for conn in connections:
            conn.close()
    
    def run_power_test(self):
        logging.info('starting power test...')
        self.power_stream.run_power()
    
    def run_throughput_test(self):
        logging.info('starting throughput test...')
        processes = []

        for stream in self.throughput_streams:
            processes.append(Process(target=stream.run_throughput))
        processes.append(Process(target=self.refresh_stream.run_refresh))
        
        for process in processes:
            process.start()
        
        for process in processes:
            process.join()
    
    def _power_result(self, qi0, ri0):
        query_res = 1
        refresh_res = 1

        for query_time in qi0:
            query_res = query_res * query_time
        for refresh_time in ri0:
            refresh_res = refresh_res * refresh_time
        
        return (3600 * self.scale_factor) / pow(query_res * refresh_res, 1/24)
    
    def _throughput_result(self, ts):
        return (self.n_query_streams * 22 * 3600 * self.scale_factor) / ts
    
    def get_results(self) -> tuple[float, float, float]:
        '''
        Compute the TPC-H performance metrics for the tests that have been executed.
        
        :returns qphh_metric: the composite query-per-hour performance metric (clause 5.4.3)
        :returns power_metric: the power metric (clause 5.4.1)
        :returns throughput_metric: the throughput metric (clause 5.4.2)
        '''
        logging.info('computing performance metrics...')
        power_result = None
        throughput_results = []

        while True:
            if self.timer_queue.empty():
                break
            result = self.timer_queue.get()

            if result['mode'] == 'power':
                power_result = result
            else:
                throughput_results.append(result)
        
        through_start = min([result['start'] for result in throughput_results])
        through_end = max([result['end'] for result in throughput_results])

        power_metric = self._power_result(power_result['qi'], power_result['ri'])
        throughput_metric = self._throughput_result(through_end - through_start)
        qphh_metric = pow(power_metric * throughput_metric, 1/2)

        return qphh_metric, power_metric, throughput_metric