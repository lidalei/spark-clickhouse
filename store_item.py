import argparse
import json
import logging

import typing

from clickhouse_driver.errors import ServerException
from pyspark import SparkContext, SparkConf
# from pyspark.streaming import StreamingContext

from clickhouse_driver import Client

LOG_FORMAT = '%(asctime)s %(process)d %(filename)s %(lineno)d %(message)s'


def create_table(server: str, sql: str):
    cli = Client(server)
    return cli.execute(sql)


def create_batch(iterator, batch_size: int):
    batch = []
    for x in iterator:
        batch.append(x)
        if len(batch) >= batch_size:
            yield batch
            batch.clear()
    # yield final batch
    if len(batch) > 0:
        yield batch


# def insert_batch(server: str, insert_sql: str, batch):
#     cli = Client(server)
#     return cli.execute(insert_sql, batch)


def insert_partition(server: str, insert_sql: str, iterator: typing.Iterable, batch_size: int):
    # set logging format and level in worker
    logging.basicConfig(format=LOG_FORMAT)
    logging.getLogger().setLevel(logging.INFO)

    cli = Client(server)
    total_rows_inserted = 0
    total_rows_failed = 0
    for batch in create_batch(iterator, batch_size):
        try:
            n = cli.execute(insert_sql, batch)
            logging.info(f'inserted {n} rows')
        except (ServerException, Exception):
            total_rows_failed += len(batch)
            logging.exception('failed to insert a batch')
        else:
            total_rows_inserted += n

    logging.info(f'in total, successfully inserted {total_rows_inserted} rows, failed to insert {total_rows_failed} rows')


def main(args: argparse.Namespace):
    conf = SparkConf().setAppName(args.app_name).setMaster(args.spark_master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    items = sc.textFile(args.items_dir)
    # print(items.first())
    # print(items.count())
    j_items = items.map(lambda line: json.loads(line))
    """
    reviewerID FixedString(14), -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
    asin FixedString(10), -- ID of the product, e.g. 0000013714
    overall UInt8, -- rating of the product, e.g. 5
    unixReviewTime DateTime('UTC') -- time of the review (unix time)
    """
    dict_items = j_items.map(lambda x: {
        'reviewerID': x['reviewerID'],
        'asin': x['asin'],
        'overall': int(x['overall']),
        'unixReviewTime': int(x['unixReviewTime'])
    })

    dict_items.foreachPartition(
        lambda iterator: insert_partition(
            args.clickhouse_server,
            f'INSERT INTO {args.items_table_name} VALUES',
            iterator,
            args.batch_size
        )
    )
    """
    # Manually insert rows in batch 
    dict_items_with_key = dict_items.map(lambda x: (random.randint(1, 100), x))
    dict_items_with_key_batch = dict_items_with_key.groupByKey().mapValues(list)
    dict_items_with_key_batch.foreach()
    """

    """
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[*]", "StoreItem")
    ssc = StreamingContext(sc, 1)
    items = ssc.textFileStream(args.items_dir)
    items.count().pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    """


if __name__ == '__main__':
    # set logging format and level in driver
    logging.basicConfig(format=LOG_FORMAT)
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--app-name',
        type=str,
        default='store_item',
        help='Spark App name shown in UI'
    )
    parser.add_argument(
        '--spark-master',
        type=str,
        default='local[*]',
        help='Spark master'
    )

    parser.add_argument(
        '--items-dir',
        type=str,
        default='data/items/',
        help='a directory which contains items json file'
    )
    parser.add_argument(
        '--metadata-dir',
        type=str,
        default='data/metadata/',
        help='a directory which contains metadata json file'
    )

    parser.add_argument(
        '--clickhouse-server',
        type=str,
        default='localhost',
        help='clickhouse server address'
    )
    parser.add_argument(
        '--items-table-name',
        type=str,
        default='items',
        help='clickhouse table name of items'
    )
    parser.add_argument(
        '--items-table-sql',
        type=str,
        default='items.sql',
        help='an SQL file which creates clickhouse table items'
    )
    parser.add_argument(
        '--batch-size',
        type=str,
        default=100,
        help='batch size when inserting rows into clickhouse table items'
    )

    args, _ = parser.parse_known_args()
    logging.info(f'args: {args}')

    with open(args.items_table_sql) as sql_file:
        sql = sql_file.read()

    res = create_table(args.clickhouse_server, sql)
    logging.info(f'successfully created table {args.items_table_name}')

    main(args)
