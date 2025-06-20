import time
from connection import Connection
from replica import Replica

class RefreshPair:
    def __init__(self, rf1_data: list[dict[str, list[str]]], rf2_data: list[str], replica: Replica):
        '''
        A class that represents a pair of refresh functions
        for a given QueryStream. 
        '''
        self.rf1_data = rf1_data
        self.rf2_data = self.generate_queries_for_rf2(rf2_data)
        self.replica = replica

    def generate_queries_for_rf2(self, rf2_data: list[str]):
        '''
        `rf2_data` is a list of `orderkey`s that must be deleted from both the
        orders and the lineitem table. Here, we precompute the query strings so
        that we don't have to use template substitution while actually in the timed
        loop (note this is permitted by TPC-H clause 2.5.3.1).

        :param rf2_data: the list of keys to delete
        :returns rf2_data: a list of queries to execute, with the keys substituted in
        '''
        queries = []

        for orderkey in rf2_data:
            queries.append('DELETE FROM ORDERS WHERE O_ORDERKEY = %s' % orderkey)
            queries.append('DELETE FROM LINEITEM WHERE L_ORDERKEY = %s' % orderkey)
        
        return queries
    
    def run_refresh_function_1(self, timer_queue):
        connection = Connection(self.replica)
        start_time = None
        with connection.conn().cursor() as cur:
            start_time = time.time()
            for orderkey in self.rf1_data:
                cur.execute(orderkey['order'])
                for lineitem in orderkey['lineitems']:
                    cur.execute(lineitem)
        end_time = time.time()
        timer_queue.put({'start': start_time, 'end': end_time})
        connection.close()
    
    def run_refresh_function_2(self, timer_queue):
        connection = Connection(self.replica)
        start_time = None
        with connection.conn().cursor() as cur:
            start_time = time.time()
            for query in self.rf2_data:
                cur.execute(query)
        end_time = time.time()
        timer_queue.put({'start': start_time, 'end': end_time})
        connection.close()
