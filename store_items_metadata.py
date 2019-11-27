import argparse
import json
import logging
import typing

from clickhouse_driver.errors import ServerException
from pyspark import SparkContext, SparkConf
# from pyspark.streaming import StreamingContext

from clickhouse_driver import Client

LOG_FORMAT = '%(asctime)s %(process)d %(filename)s %(lineno)d %(message)s'


def create_tables(server: str, sqls: typing.List[str]) -> typing.List[Exception]:
    cli = Client(server)
    exceptions = []
    for sql in sqls:
        try:
            cli.execute(sql)
        except (ServerException, Exception) as e:
            exceptions.append(e)
        else:
            exceptions.append(None)

    return exceptions


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


def same_also_bought_also_viewed(product: dict):
    """Check if also_bought it identical to also_viewed in a product metadata"""
    if 'related' not in product:
        return True

    related = product['related']
    has_bought = 'also_bought' in related
    has_viewed = 'also_viewed' in related
    if has_bought != has_viewed:  # only one exists
        return False

    if not has_bought and not has_viewed:  # neither exists
        return True

    return set(related['also_viewed']) == set(related['also_viewed'])


def main(args: argparse.Namespace):
    # create items and metadata table
    with open(args.items_table_sql) as sql_file:
        items_sql = sql_file.read()

    with open(args.metadata_table_sql) as sql_file:
        metadata_sql = sql_file.read()

    tables = [args.items_table_name, args.metadata_table_name]
    sqls = [items_sql, metadata_sql]
    exceptions = create_tables(args.clickhouse_server, sqls)
    for table, e in zip(tables, exceptions):
        if e is not None:
            logging.error(f'failed to create table using {table}, exception: {e}')
            return

    logging.info(f'successfully created tables {tables}')

    # parse items json files
    conf = SparkConf().setAppName(args.app_name).setMaster(args.spark_master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    # gzip file cannot be split. So only one worker is utilized.
    #  We need to repartition RDD to parallelize.
    items = sc.textFile(args.items_dir)
    partitioned_items = items.repartition(4)
    j_items = partitioned_items.map(lambda line: json.loads(line))
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
            f'INSERT INTO {args.items_table_name} (reviewerID, asin, overall, unixReviewTime) VALUES',
            iterator,
            args.batch_size
        )
    )

    # parse metadata json files
    metadata = sc.textFile(args.metadata_dir)
    partitioned_metadata = metadata.repartition(4)
    d_metadata = partitioned_metadata.map(lambda line: eval(line))
    # print(d_metadata.sample(withReplacement=False, fraction=0.01).collect())
    """
    asin String, -- ID of the product, e.g. 0000013714
    -- price_in_cents Nullable(UInt32), -- price in unit of cents, e.g. 5.30 is stored as 530
    price Nullable(Decimal(10, 2)), -- price in decimal
    same_viewed_bought UInt8 -- if also_bought identical to also_viewed, 0 means no, any other value means yes
    """
    dict_metadata = d_metadata.map(lambda x: {
        'asin': x['asin'],
        # we know the price has two digits in fraction and can be represented precisely in float
        'price_in_cents': int(100 * x['price']) if 'price' in x else None,
        'same_viewed_bought': same_also_bought_also_viewed(x),
    })

    dict_metadata.foreachPartition(
        lambda iterator: insert_partition(
            args.clickhouse_server,
            f'INSERT INTO {args.metadata_table_name} (asin, price_in_cents, same_viewed_bought) VALUES',
            iterator,
            args.batch_size
        )
    )

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
        '--metadata-table-name',
        type=str,
        default='metadata',
        help='clickhouse table name of metadata'
    )
    parser.add_argument(
        '--metadata-table-sql',
        type=str,
        default='metadata.sql',
        help='an SQL file which creates clickhouse table metadata'
    )
    parser.add_argument(
        '--batch-size',
        type=str,
        default=100,
        help='batch size when inserting rows into clickhouse table items'
    )

    args, _ = parser.parse_known_args()
    logging.info(f'args: {args}')

    main(args)
