import argparse
import json
import logging
import multiprocessing
import typing
from datetime import datetime

from clickhouse_driver.errors import ServerException, LogicalError, NetworkError
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from retry import retry
from clickhouse_driver import Client

LOG_FORMAT = '%(asctime)s %(process)d %(filename)s %(lineno)d %(message)s'


class ClickhouseClient(object):
    def __init__(self, host, logfmt, loglvl=logging.INFO):
        self.host = host
        # set logging format and level in driver / worker
        logging.basicConfig(format=logfmt)
        logging.getLogger().setLevel(loglvl)

        self.cli = Client.from_url(host)
        # perform a test
        self.test()

    @retry((NetworkError,), tries=5, delay=1, backoff=2, jitter=1, max_delay=10)
    def test(self):
        res = self.cli.execute('SELECT 1 + 1')
        if res[0][0] != 2:
            raise LogicalError(f'1 + 1 = {res[0][0]}')

    def execute_sqls(self, sqls: typing.List[str]) -> typing.List[Exception]:
        """connect to clickhouse server and execute given sql statements """
        exceptions = []
        for sql in sqls:
            try:
                self.cli.execute(sql)
            except (ServerException, Exception) as e:
                exceptions.append(e)
            else:
                exceptions.append(None)

        return exceptions

    def insert_partition(self, insert_sql: str, iterator: typing.Iterable, batch_size: int):
        total_rows_inserted = 0
        total_rows_failed = 0
        for batch in create_batch(iterator, batch_size):
            try:
                n = self.cli.execute(insert_sql, batch)
                logging.info(f'inserted {n} rows')
            except (ServerException, Exception):
                total_rows_failed += len(batch)
                logging.exception('failed to insert a batch')
            else:
                total_rows_inserted += n

        logging.info(
            f'in total, successfully inserted {total_rows_inserted} rows, failed to insert {total_rows_failed} rows')


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
    # construct clickhouse host url
    ck_host = f'clickhouse://{args.clickhouse_username}:{args.clickhouse_password}@{args.clickhouse_server}/default'
    ck_cli = ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO)
    # create items and metadata table
    with open(args.items_table_sql) as sql_file:
        items_sql = sql_file.read()

    with open(args.metadata_table_sql) as sql_file:
        metadata_sql = sql_file.read()

    tables = [args.items_table_name, args.metadata_table_name]
    sqls = [items_sql, metadata_sql]
    exceptions = ck_cli.execute_sqls(sqls)
    for table, e in zip(tables, exceptions):
        if e is not None:
            logging.error(
                f'failed to create table using {table}, exception: {e}')
            return

    logging.info(f'successfully created tables {tables}')

    # parse items json files
    conf = SparkConf().setAppName(args.app_name).setMaster(args.spark_master).set(
        'spark.executor.memory', '4g'
    ).set(
        'spark.driver.memory', '4g'
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    # gzip file cannot be split. So only one worker is utilized.
    #  We need to repartition RDD to parallelize.
    items = sc.textFile(args.items_dir)
    j_items = items.map(lambda line: json.loads(line))
    # FIXME!
    # partitioned_items = items.repartition(args.num_partitions)
    # j_items = partitioned_items.map(lambda line: json.loads(line))
    """
    reviewerID FixedString(14), -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
    asin FixedString(10), -- ID of the product, e.g. 0000013714
    overall UInt8, -- rating of the product, e.g. 5
    unixReviewTime DateTime('UTC') -- time of the review (unix time)
    """
    cleaned_j_items = j_items.filter(
        lambda x: ('reviewerID' in x) and ('asin' in x) and ('overall' in x)
    )
    dict_items = cleaned_j_items.map(lambda x: {
        'reviewerID': x['reviewerID'],
        'asin': x['asin'],
        'overall': int(x['overall']),
        'unixReviewTime': int(x['unixReviewTime']) if 'unixReviewTime' in x else (
            int(datetime.strptime(
                x['reviewTime'], '%m %d, %Y').timestamp()) if 'reviewTime' in x else 0
        )
    })

    # we establish a connection for each partition
    dict_items.foreachPartition(
        lambda iterator: ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO).insert_partition(
            f'INSERT INTO {args.items_table_name} (reviewerID, asin, overall, unixReviewTime) VALUES',
            iterator,
            args.batch_size
        )
    )

    # parse metadata json files
    metadata = sc.textFile(args.metadata_dir)
    d_metadata = metadata.map(lambda line: eval(line))
    # FIXME!
    # partitioned_metadata = metadata.repartition(args.num_partitions)
    # d_metadata = partitioned_metadata.map(lambda line: eval(line))
    # print(d_metadata.sample(withReplacement=False, fraction=0.01).collect())
    """
    asin String, -- ID of the product, e.g. 0000013714
    -- price_in_cents Nullable(UInt32), -- price in unit of cents, e.g. 5.30 is stored as 530
    price Nullable(Decimal(10, 2)), -- price in decimal
    same_viewed_bought UInt8 -- if also_bought identical to also_viewed, 0 means no, any other value means yes
    """
    cleaned_d_metadata = d_metadata.filter(
        lambda x: 'asin' in x
    )
    dict_metadata = cleaned_d_metadata.map(lambda x: {
        'asin': x['asin'],
        # we know the price has two digits in fraction and can be represented precisely in float
        'price_in_cents': int(100 * x['price']) if 'price' in x else None,
        'same_viewed_bought': same_also_bought_also_viewed(x),
    })

    dict_metadata.foreachPartition(
        lambda iterator: ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO).insert_partition(
            f'INSERT INTO {args.metadata_table_name} (asin, price_in_cents, same_viewed_bought) VALUES',
            iterator,
            args.batch_size
        )
    )


def stream_main(args: argparse.Namespace):
    # construct clickhouse host url
    ck_host = f'clickhouse://{args.clickhouse_username}:{args.clickhouse_password}@{args.clickhouse_server}/default'
    ck_cli = ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO)
    # create items and metadata table
    with open(args.items_table_sql) as sql_file:
        items_sql = sql_file.read()

    with open(args.metadata_table_sql) as sql_file:
        metadata_sql = sql_file.read()

    tables = [args.items_table_name, args.metadata_table_name]
    sqls = [items_sql, metadata_sql]
    exceptions = ck_cli.execute_sqls(sqls)
    for table, e in zip(tables, exceptions):
        if e is not None:
            logging.error(
                f'failed to create table using {table}, exception: {e}')
            return

    logging.info(f'successfully created tables {tables}')

    conf = SparkConf().setAppName(args.app_name).setMaster(args.spark_master).set(
        'spark.executor.memory', '2g'
    ).set(
        'spark.driver.memory', '2g'
    )
    sc = SparkContext(conf=conf)
    # Create a local StreamingContext with two working thread and batch interval of 10 seconds
    # FIXME! 10s -> XXX
    ssc = StreamingContext(sc, batchDuration=10)
    items = ssc.textFileStream(args.items_dir)
    items.count().pprint()

    j_items = items.map(lambda line: json.loads(line))
    dict_items = j_items.map(lambda x: {
        'reviewerID': x['reviewerID'],
        'asin': x['asin'],
        'overall': int(x['overall']),
        'unixReviewTime': int(x['unixReviewTime'])
    })

    # we establish a connection for each partition
    dict_items.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda iterator: ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO).insert_partition(
                f'INSERT INTO {args.items_table_name} (reviewerID, asin, overall, unixReviewTime) VALUES',
                iterator,
                args.batch_size
            )
        )
    )

    ssc.start()             # Start the computation

    # rename file
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
    # set logging format and level in driver
    logging.basicConfig(format=LOG_FORMAT)
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--app-name',
        type=str,
        default='Store Items and Metadata',
        help='Spark App name shown in UI'
    )
    parser.add_argument(
        '--spark-master',
        type=str,
        default='local[*]',
        help='Spark master'
    )
    # parser.add_argument(
    #     '--num-partitions',
    #     type=int,
    #     default=multiprocessing.cpu_count(),
    #     help='number of partitions to parallelize processing, e.g. cpu cores * number of workers'
    # )

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
        default='localhost:9000',
        help='clickhouse server address'
    )
    parser.add_argument(
        '--clickhouse-username',
        type=str,
        default='default',
        help='clickhouse server username'
    )
    parser.add_argument(
        '--clickhouse-password',
        type=str,
        default='',
        help='clickhouse server password'
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
        type=int,
        default=1000,
        help='batch size when inserting rows into clickhouse table items'
    )

    args, _ = parser.parse_known_args()
    logging.info(f'args: {args}')

    # stream_main(args)
    main(args)
