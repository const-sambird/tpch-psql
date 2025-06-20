import psycopg
from multiprocessing import Process
from connection import Connection
from replica import Replica
from refresh_pair import RefreshPair

class QueryStream:
    def __init__(self, replicas: list[Replica], queries: list[str], routes: list[int], order: list[int], rf1_data, rf2_data):
        '''
        Represents a single query stream for the TPC-H benchmark.

        :param replicas: the database replicas in this (distributed) DBMS
        :param queries: the TPC-H queries, in canonical order (1-22)
        :param routes: which replica each query ought to be routed to
        :param order: the order in which we should execute the queries in this stream (from appendix A of the TPC-H spec)
        :param rf1_data: data for the first refresh function (see generator for spec)
        :param rf2_data: data for the second refresh function (see generator for spec)
        '''
        self.replicas = replicas
        self.connections = [Connection(replica) for replica in replicas]
        self.cursors = [c.conn().cursor() for c in self.connections]
        self.refresh_pairs = [RefreshPair(rf1_data, rf2_data, replica) for replica in replicas]

        # here, a potentially misguided attempt to reorder the queries for this stream in advance
        # does this help with data locality? who knows it probably doesn't matter that much
        self.queries = [queries[i] for i in order]
        self.routes = [routes[i] for i in order]
    
    def run(self):
        '''
        Run this query stream.
        '''
        self._run_refresh_function_1()
        self._run_query_set()
        self._run_refresh_function_2()

        self._cleanup()

    def _run_query_set(self):
        for i in range(22):
            replica = self.routes[i]
            self.cursors[replica].execute(self.queries[i])
    
    def _run_refresh_function_1(self):
        processes = [Process(target=r.run_refresh_function_1) for r in self.refresh_pairs]
        [p.start() for p in processes]
        [p.join() for p in processes]

    def _run_refresh_function_2(self):
        processes = [Process(target=r.run_refresh_function_2) for r in self.refresh_pairs]
        [p.start() for p in processes]
        [p.join() for p in processes]
    
    def _cleanup(self):
        for cursor in self.cursors:
            cursor.close()
        for connection in self.connections:
            connection.close()