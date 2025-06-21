import glob
import os
import subprocess
import shutil
import logging

from replica import Replica
from connection import Connection

class Generator:
    def __init__(self, replicas: list[Replica], dbgen_path: str, data_path: str, scale_factor: int, n_query_streams: int):
        '''
        TPC-H data generator. Interface to the `dbgen` tool provided by
        the Transaction Processing Performance Council.

        `dbgen` should be downloaded in the directory given by `dbgen_path` and
        a Makefile created, but this program will recompile and execute dbgen
        with the given `scale_factor`.

        After the data is created, it can be loaded into the specified database
        replicas.

        Used QRLIT as a reference: https://github.com/DBarbosaDev/QRLIT/blob/main/db_env/tpch/TpchGenerator.py

        :param replicas: the databases
        :param dbgen_path: the location of the directory where dbgen will be compiled (fully qualified)
        :param data_path: the desired data directory for the output table data/queries/refresh functions
        :param scale_factor: the TPC-H scale factor (usually 10 for our experiments)
        :param n_query_streams: number of query streams (information needed for the refresh function)
        '''
        self.replicas = replicas
        self.dbgen_path = dbgen_path
        self.data_path = data_path
        self.scale_factor = str(scale_factor)
        self.n_query_streams = n_query_streams
        self.dbname = replicas[0].dbname
    
    def generate(self):
        '''
        Generates the table, query, and refresh data for the
        TPC-H benchmark, according to the parameters that were
        passed to the constructor of the Generator class.

        Creates the specified data directory (if it does not already exist),
        compiles dbgen (or recompiles it), and generates the data.
        '''
        self._create_directories()
        self._compile_dbgen()
        self._create_table_data()
        self._format_table_data()
        self._create_refresh_data()
        self._create_queries()

    def load(self) -> tuple[list[str], list[list[dict[str, list[str]]]], list[list[str]]]:
        '''
        Loads the data into the database, or memory, as appropriate.

        (1) Resets each database, creates the schemas, and loads the generated table data.

        (2) Creates the default primary key/foreign key constraints on those tables.
        
        (3) Loads the query text to be executed.

        (4) Loads the refresh pairs and sets them up for execution.

        The rf*_data objects are lists, one for each refresh pair. The data structuring is a bit horrific,
        but we need to serialise it to pass them to the spawned subprocesses.

        :returns queries: the 22 generated queries to be executed according to the stream order given in the specification
        :returns rf1_data: the data for the first refresh function: a dict with a new ORDER and a list of new LINEITEMs
        :returns rf2_data: the data for the second refresh function: a list of orderkeys to delete from ORDERS and LINEITEM
        '''
        connections = [Connection(replica) for replica in self.replicas]
        tables = []

        for table_file in glob.glob(f'{self.data_path}/tables/*.tbl'):
            name = os.path.basename(table_file)
            tables.append(name.split('.')[0])

        self._reset_database(connections, tables)
        self._create_schemas(connections)
        self._load_table_data(connections)
        self._create_keys(connections)

        for c in connections:
            c.close()

        return self._load_queries(), self._load_rf1_data(), self._load_rf2_data()
    
    def _create_directories(self):
        logging.debug(f'creating data directories under {self.data_path}')
        os.makedirs(f'{self.data_path}/refresh', exist_ok=True)
        os.makedirs(f'{self.data_path}/tables', exist_ok=True)
        os.makedirs(f'{self.data_path}/queries', exist_ok=True)
        os.makedirs(f'{self.data_path}/schema', exist_ok=True)

    def _compile_dbgen(self):
        logging.debug(f'attempting to compile TPC-H dbgen at {self.dbgen_path}')
        subprocess.run('make', cwd=self.dbgen_path)
    
    def _create_table_data(self):
        logging.debug(f'creating table data for scale factor {self.scale_factor}')
        subprocess.run([f'{self.dbgen_path}/dbgen', '-s', self.scale_factor, '-vf'], cwd=self.dbgen_path)

        table_paths = glob.glob('*.tbl', root_dir=self.dbgen_path)

        for file in table_paths:
            shutil.move(f'{self.dbgen_path}/{file}', f'{self.data_path}/tables/{os.path.basename(file)}')

        shutil.copy(f'{self.dbgen_path}/dss.ddl', f'{self.data_path}/schema/dss.ddl')

        root_dir = os.path.dirname(os.path.realpath(__file__))
        shutil.copy(f'{root_dir}/schema_keys.sql', f'{self.data_path}/schema/schema_keys.sql')
    
    def _create_refresh_data(self):
        logging.debug(f'creating refresh function data for scale factor {self.scale_factor}')
        subprocess.run([f'{self.dbgen_path}/dbgen', '-s', self.scale_factor, '-vf', '-U', str(self.n_query_streams)], cwd=self.dbgen_path)

        update_paths = glob.glob('*.tbl.u*', root_dir=self.dbgen_path)
        delete_paths = glob.glob('delete.*', root_dir=self.dbgen_path)
        update_paths.extend(delete_paths)

        for file in update_paths:
            shutil.move(f'{self.dbgen_path}/{file}', f'{self.data_path}/refresh/{os.path.basename(file)}')

    def _create_queries(self):
        logging.debug(f'creating TPC-H query data')

        for i in range(1, 23):
            with open(f'{self.data_path}/queries/{i}.sql', 'w') as outfile:
                subprocess.run([f'{self.dbgen_path}/qgen', '-s', self.scale_factor],
                               cwd=self.dbgen_path,
                               env=dict(os.environ, DSS_QUERY=f'{self.dbgen_path}/queries'),
                               stdout=outfile)
    
    def _reset_database(self, connections: list[Connection], tables: list[str]):
        '''
        Drops the tables specified.

        :param tables: a list of table names
        '''
        logging.debug(f'dropping existing tables: {tables}')
        for c in connections:
            with c.conn().cursor() as cur:
                for table in tables:
                    cur.execute(f'DROP TABLE IF EXISTS {table}')

    def _create_schemas(self, connections: list[Connection]):
        logging.info('creating the schemas for tables')
        for c in connections:
            with c.conn().cursor() as cur:
                with open(f'{self.data_path}/schema/dss.ddl', 'r') as infile:
                    cur.execute(infile.read())

    def _create_keys(self, connections: list[Connection]):
        logging.info('creating primary and foreign keys')
        for c in connections:
            with c.conn().cursor() as cur:
                with open(f'{self.data_path}/schema/schema_keys.sql', 'r') as infile:
                    cur.execute(infile.read())
    
    def _format_table_data(self):
        '''
        The table data by default includes a trailing pipe (|) character
        that must be removed for Postgres to process it correctly with the
        `COPY FROM ... FORMAT CSV` command. We do that in-place here.
        '''
        logging.debug('correcting table data formats')
        for table_file in glob.glob(f'*.tbl', root_dir=f'{self.data_path}/tables'):
            logging.debug(table_file)
            subprocess.run(['sed', '-i', 's/.$//', table_file], cwd=f'{self.data_path}/tables')
    
    def _load_table_data(self, connections: list[Connection]):
        for table_file in glob.glob(f'{self.data_path}/tables/*.tbl'):
            table = os.path.basename(table_file).split('.')[0]
            logging.info(f'loading data into {table}')
            for num, c in enumerate(connections):
                logging.debug(f'loading to replica {num}')
                with c.conn().cursor() as cur:
                    with cur.copy(f'COPY {table} FROM STDIN (format csv, delimiter \'|\')') as copy:
                        with open(table_file, 'r') as input:
                            copy.write(input.read())

    def _load_queries(self) -> list[str]:
        logging.info('reading queries')
        queries = []

        for query_file in glob.glob(f'{self.data_path}/queries/*.sql'):
            with open(query_file, 'r') as infile:
                queries.append(infile.read())
        
        return queries

    def _load_rf1_data(self) -> list[dict[str, list[str]]]:
        logging.info('loading data for refresh function #1')

        rf1_data = []

        for i in range(self.n_query_streams):
            lineitems = open(f'{self.data_path}/refresh/lineitem.tbl.u{i + 1}')
            orders = open(f'{self.data_path}/refresh/orders.tbl.u{i + 1}')

            stream_order = []
            stream_data = {}

            for order in orders:
                this_entry = {
                    'order': order,
                    'lineitems': []
                }

                orderkey = order.split('|')[0]
                stream_order.append(orderkey)
                stream_data[orderkey] = this_entry[:-1]
            
            for lineitem in lineitems:
                orderkey = lineitem.split('|')[0]
                stream_data[orderkey]['lineitems'].append(lineitem[:-1])
            
            lineitems.close()
            orders.close()

            rf1_data.append([stream_data[orderkey] for orderkey in stream_order])
        
        return rf1_data

    def _load_rf2_data(self) -> list[list[str]]:
        logging.info('loading data for refresh function #2')

        rf2_data = []

        for i in range(self.n_query_streams):
            with open(f'{self.data_path}/refresh/delete.{i + 1}', 'r') as infile:
                rf2_data.append([orderkey[:-1] for orderkey in infile.readlines()]) # trim trailing pipe character
        
        return rf2_data
