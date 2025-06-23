import logging
import time
from multiprocessing import Process, Queue
from connection import Connection
from replica import Replica
from refresh_pair import RefreshPair

class QueryStream:
    def __init__(self, num: int, replicas: list[Replica], queries: list[str], routes: list[int], order: list[int], rf1_data, rf2_data, num_refresh_pairs: int, timer_queue: Queue):
        '''
        Represents a single query stream for the TPC-H benchmark.

        :param num: which query stream is this?
        :param replicas: the database replicas in this (distributed) DBMS
        :param queries: the TPC-H queries, in canonical order (1-22)
        :param routes: which replica each query ought to be routed to
        :param order: the order in which we should execute the queries in this stream (from appendix A of the TPC-H spec)
        :param rf1_data: data for the first refresh function (see generator for spec)
        :param rf2_data: data for the second refresh function (see generator for spec)
        :param num_refresh_pairs: how many refresh pairs should be run in this query stream? (1 for power, num_streams for refresh, 0 for throughput)
        :param timer_queue: the queue to put the timing data in back to the main process
        '''
        self.num = num
        self.replicas = replicas
        self.connections = [Connection(replica) for replica in replicas]
        self.cursors = [c.conn().cursor() for c in self.connections]
        self.num_refresh_pairs = num_refresh_pairs
        self.refresh_pairs = [[RefreshPair(rf1_data[i], rf2_data[i], replica) for replica in replicas] for i in range(num_refresh_pairs)]

        self.queries = queries
        self.routes = routes
        self.order = order

        # we need to pass timing intervals, both back to the main process
        # and from our own refresh stream subprocesses we spawn
        self.timer_queue = timer_queue
        self.refresh_time_queue = Queue()
        self.start_time = None
        self.query_times = [None for _ in range(22)]
        self.refresh_times = [None, None]
        self.end_time = None
        self.mode = None
    
    def run_power(self):
        '''
        Run this query stream in POWER TEST mode.
        '''
        self.mode = 'power'
        self._run_refresh_function_1(0)
        self._run_query_set()
        self._run_refresh_function_2(0)

        self._finalise()
    
    def run_throughput(self):
        '''
        Run this query stream in THROUGHPUT TEST mode (don't execute the refresh functions -- they'll be run by a separate stream).
        '''
        self.mode = 'throughput'
        self._run_query_set()
        
        self._finalise()
    
    def run_refresh(self):
        '''
        Run REFRESH STREAMS only.
        '''
        self.mode = 'refresh'
        for i in range(self.num_refresh_pairs):
            self._run_refresh_function_1(i)
            self._run_refresh_function_2(i)

        self._finalise()

    def _run_query_set(self):
        for i in range(22):
            execute = self.order[i] - 1
            replica = self.routes[execute]
            tic = time.time()
            self.cursors[replica].execute(self.queries[execute])
            toc = time.time()
            logging.debug(f'QS{self.num}:Q{execute + 1} : {round(toc - tic, 2)}s')
            self.query_times[execute] = toc - tic

    
    def _run_refresh_function_1(self, i):
        logging.debug(f'starting refresh function #1 in query stream {self.num}:I{i}')
        processes = [Process(target=r.run_refresh_function_1, args=(self.refresh_time_queue,)) for r in self.refresh_pairs[i]]
        [p.start() for p in processes]
        [p.join() for p in processes]
        times = [self.refresh_time_queue.get() for _ in processes]
        start_time = min([time['start'] for time in times])
        end_time = max([time['end'] for time in times])
        logging.debug(f'QS{self.num}:I{i}:RF1 : {round(end_time - start_time, 2)}s')
        if self.start_time is None:
            self.start_time = start_time
        self.refresh_times[0] = end_time - start_time

    def _run_refresh_function_2(self, i):
        logging.debug(f'starting refresh function #2 in query stream {self.num}:I{i}')
        processes = [Process(target=r.run_refresh_function_2, args=(self.refresh_time_queue,)) for r in self.refresh_pairs[i]]
        [p.start() for p in processes]
        [p.join() for p in processes]
        times = [self.refresh_time_queue.get() for _ in processes]
        start_time = min([time['start'] for time in times])
        end_time = max([time['end'] for time in times])
        logging.debug(f'QS{self.num}:I{i}:RF2 : {round(end_time - start_time, 2)}s')
        self.end_time = end_time
        self.refresh_times[1] = end_time - start_time
    
    def _finalise(self):
        for cursor in self.cursors:
            cursor.close()
        for connection in self.connections:
            connection.close()
        self.timer_queue.put({
            'mode': self.mode,
            'start': self.start_time,
            'end': self.end_time,
            'qi': self.query_times,
            'ri': self.refresh_times
        })
