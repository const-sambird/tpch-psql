import argparse
import logging
import os

from benchmark import Benchmark
from replica import Replica
from generator import Generator

def create_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--scale-factor', type=int, default=10, help='the TPC-H scale factor')
    parser.add_argument('-g', '--dbgen-dir', type=str, default='./tpc-h/dbgen', help='the path to the TPC-H tools dbgen directory')
    parser.add_argument('-d', '--data-dir', type=str, default='./data', help='the path where the data generated should be stored')
    parser.add_argument('-r', '--replicas', type=str, default='replicas.csv', help='the CSV file with replica connection details')
    parser.add_argument('-i', '--index-config', type=str, default='config.csv', help='the path to the index configuration')
    parser.add_argument('-t', '--routing-table', type=str, default='routes.csv', help='the path to the routing table')
    parser.add_argument('-q', '--query-streams', type=int, help='number of query streams to run (if omitted, minimum given in spec will be used)')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose log output')

    return parser.parse_args()

def get_default_query_streams(scale_factor: int) -> int:
    if scale_factor < 10:
        return 2
    elif scale_factor < 30:
        return 3
    elif scale_factor < 100:
        return 4
    elif scale_factor < 300:
        return 5
    elif scale_factor < 1000:
        return 6
    elif scale_factor < 3000:
        return 7
    elif scale_factor < 10000:
        return 8
    elif scale_factor < 30000:
        return 9
    elif scale_factor < 100000:
        return 10
    else:
        return 11

def table_from_column_prefix(column: str) -> str:
    '''
    Given the name of a column in the TPC-H benchmark,
    returns the name of the table it is on based on the
    prefix attached to the column name.

    :param column: the column name (eg ps_suppkey)
    :returns: the table name (eg PARTSUPP)
    '''
    PREFIXES = {
        'l': 'LINEITEM',
        'p': 'PART',
        'ps': 'PARTSUPP',
        'o': 'ORDERS',
        'c': 'CUSTOMER',
        'n': 'NATION',
        'r': 'REGION',
        's': 'SUPPLIER'
    }
    prefix = column.split('_')[0]

    return PREFIXES[prefix]

def get_replicas(path: str):
    replicas = []
    with open(path, 'r') as infile:
        lines = infile.readlines()
        for config in lines:
            fields = config.split(',')
            replicas.append(
                Replica(
                    id=fields[0],
                    hostname=fields[1],
                    port=fields[2],
                    dbname=fields[3],
                    user=fields[4],
                    password=fields[5]
                )
            )
    return replicas

def get_index_config(path: str, num_replicas: int) -> list[tuple[str]]:
    indexes = []
    for replica in range(num_replicas):
        indexes.append([])
    
    with open(path, 'r') as infile:
        lines = infile.readlines()
        for index in lines:
            fields = index.split(',')
            to_replica = int(fields[0])
            table = table_from_column_prefix(fields[1])
            indexes[to_replica].append([table, fields[1:]])
    
    return indexes

def get_routes(path: str) -> list[int]:
    routes = None

    with open(path, 'r') as infile:
        table = infile.readline()
        routes = table.split(',')
        routes = [int(r) for r in routes]
    
    return routes

if __name__ == '__main__':
    args = create_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    replicas = get_replicas(args.replicas)
    config = get_index_config(args.index_config, len(replicas))
    routes = get_routes(args.routing_table)

    if args.query_streams is None:
        num_query_streams = get_default_query_streams()
    else:
        num_query_streams = args.query_streams

    base = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.join(base, args.data_dir)
    dbgen_dir = os.path.join(base, args.dbgen_dir)

    generator = Generator(replicas, dbgen_dir, data_dir, args.scale_factor, num_query_streams + 1)

    logging.info(f'generating TPC-H data, scale factor {args.scale_factor}')
    generator.generate()

    logging.info('loading TPC-H data')
    queries, rf1_data, rf2_data = generator.load()

    benchmark = Benchmark(queries, rf1_data, rf2_data, replicas, routes, config, num_query_streams)

    benchmark.run_power_test()
    benchmark.run_throughput_test()

    qphh, power, throughput = benchmark.get_results()

    logging.info('=' * 30)
    logging.info('TPC-H Performance Benchmark Results')
    logging.info('')
    logging.info(f'Power@Size       = {round(power, 3)}')
    logging.info(f'Throughput@Size  = {round(throughput, 3)}')
    logging.info(f'QphH@Size        = {round(qphh, 3)}')
    logging.info('')
    logging.info(f'Scale factor: {args.scale_factor}')
    logging.info('=' * 30)
