import logging
from multiprocessing import Process
from connection import Connection
from replica import Replica
from query_stream import QueryStream

from tpc_const import QS_ORDER

class Benchmark:
    def __init__(self, queries: list[str], rf1_data, rf2_data, replicas: list[Replica], routes: list[int], indexes: list[list[str, list[str]]], n_query_streams: int):
        self.queries = queries
        self.replicas = replicas
        self.routes = routes
        self.indexes = indexes

        self.power_stream = QueryStream(replicas, queries, routes, QS_ORDER[0], rf1_data, rf2_data)
        self.throughput_streams = []

        for i in range(1, n_query_streams + 1):
            self.throughput_streams.append(QueryStream(replicas, queries, routes, QS_ORDER[i], rf1_data, rf2_data))
        
        self._create_indexes()
    
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
        self.power_stream.run()
    
    def run_throughput_test(self):
        logging.info('starting throughput test...')
        processes = []

        for stream in self.throughput_streams:
            processes.append(Process(target=stream.run))
        
        for process in processes:
            process.start()
        
        for process in processes:
            process.join()