import psycopg
import logging
from replica import Replica

class Connection:
    def __init__(self, replica: Replica):
        '''
        Represents a single connection to each database.

        Due to the nature of Python's multiprocessing and psycopg's
        `Connection` object, each database within each query stream will
        require its own connection to the database (at scale factor 10
        with 3 query streams and 2 replicas, that is 6 connections total,
        but they're not all going to the same database, so that isn't too
        much).

        Connections should only be created from *within* each spawned process
        (typically from the `QueryStream` class), presuming they're being used
        in a multiprocess context.

        :param replica: the database `Replica` we are connecting to
        '''
        self.replica = replica
        self._connection = psycopg.connect(replica.connection_string)
    
    def conn(self):
        if self._connection is not None:
            return self._connection
        
        logging.error(f'connecting to replica {self.replica.id}: connection has already been closed!')
        return None
    
    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
