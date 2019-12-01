import argparse
import json
import logging
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from download import download_unzip_large_gz_file
from clickhouse_client import ClickhouseClient

LOG_FORMAT = '%(asctime)s %(process)d %(filename)s %(lineno)d %(message)s'


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


def parse_review_time(x: dict) -> int:
    try:
        if 'unixReviewTime' in x:
            return int(x['unixReviewTime'])

        if 'reviewTime' in x:
            return int(datetime.strptime(x['reviewTime'], '%m %d, %Y').timestamp())
    except:
        return 0


def process_metadata(sc: SparkContext, path_to_file: str, ck_host: str, ck_table_name: str):
    # parse metadata json files
    metadata = sc.textFile(path_to_file)
    d_metadata = metadata.map(lambda line: eval(line))
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
            f'INSERT INTO {ck_table_name} (asin, price_in_cents, same_viewed_bought) VALUES',
            iterator,
        )
    )


def process_items(sc: SparkContext, path_to_file: str, ck_host: str, ck_table_name: str):
    # parse items json files and insert into clickhouse
    items = sc.textFile(path_to_file)
    j_items = items.map(lambda line: json.loads(line))
    cleaned_j_items = j_items.filter(
        lambda x: ('asin' in x) and ('overall' in x)
    )
    dict_items = cleaned_j_items.map(lambda x: {
        'reviewerID': x['reviewerID'] if 'reviewerID' in x else None,
        'asin': x['asin'],
        'overall': int(x['overall']),
        'unixReviewTime': parse_review_time(x),
    })

    # we establish a connection for each partition
    dict_items.foreachPartition(
        lambda iterator: ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO).insert_partition(
            f'INSERT INTO {ck_table_name} (reviewerID, asin, overall, unixReviewTime) VALUES',
            iterator,
        )
    )


def _process_items_stream(sc: SparkContext, items_dir: str, ck_host: str, ck_table_name: str):
    """A streaming solution, not working yet"""
    # Create a local StreamingContext with two working thread and batch interval of 10 seconds
    ssc = StreamingContext(sc, batchDuration=10)
    items = ssc.textFileStream(items_dir)

    j_items = items.map(lambda line: json.loads(line))
    cleaned_j_items = j_items.filter(
        lambda x: ('asin' in x) and ('overall' in x)
    )
    dict_items = cleaned_j_items.map(lambda x: {
        'reviewerID': x['reviewerID'] if 'reviewerID' in x else None,
        'asin': x['asin'],
        'overall': int(x['overall']),
        'unixReviewTime': parse_review_time(x),
    })

    # we establish a connection for each partition
    dict_items.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda iterator: ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO).insert_partition(
                f'INSERT INTO {ck_table_name} (reviewerID, asin, overall, unixReviewTime) VALUES',
                iterator,
            )
        )
    )

    ssc.start()             # Start the computation
    # FIXME! download file in order to start to process
    ssc.awaitTermination()  # Wait for the computation to terminate


def main(args: argparse.Namespace):
    # construct clickhouse host url
    ck_host = f'clickhouse://{args.clickhouse_user}:{args.clickhouse_password}@{args.clickhouse_server}/{args.clickhouse_db}'
    ck_cli = ClickhouseClient(ck_host, LOG_FORMAT, loglvl=logging.INFO)
    # check if items and metadata table exist
    tables = [args.items_table_name, args.metadata_table_name]
    missing_tables = ck_cli.tables_nonexist(tables)
    if len(missing_tables) > 0:
        logging.error(f'tables {missing_tables} do not exist, create them and retry later')
        return

    # create SparkContext
    conf = SparkConf().setAppName(args.app_name).setMaster(args.spark_master).set(
        'spark.executor.memory', args.spark_executor_memory
    ).set(
        'spark.driver.memory', args.spark_driver_memory
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    # download metadata file and write data into a clickhouse table
    download_unzip_large_gz_file(args.metadata_uri, args.metadata_file)
    process_metadata(sc, args.metadata_file, ck_host, args.metadata_table_name)

    # download items file and write data into a clickhouse table
    download_unzip_large_gz_file(args.items_uri, args.items_file)
    process_items(sc, args.items_file, ck_host, args.items_table_name)


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
    parser.add_argument(
        '--spark-driver-memory',
        type=str,
        default='6g',
        help='Spark driver memory'
    )
    parser.add_argument(
        '--spark-executor-memory',
        type=str,
        default='6g',
        help='Spark executor memory'
    )
    # parser.add_argument(
    #     '--num-partitions',
    #     type=int,
    #     default=multiprocessing.cpu_count(),
    #     help='number of partitions to parallelize processing, e.g. cpu cores * number of workers'
    # )

    parser.add_argument(
        '--items-uri',
        type=str,
        default='data/items/item_dedup_sample.json',
        help='uri of a gzipped items json file '
    )
    parser.add_argument(
        '--items-file',
        type=str,
        default='data/items/item_dedup_sample.json',
        help='path to an items json file'
    )
    parser.add_argument(
        '--metadata-uri',
        type=str,
        default='data/metadata/metadata_sample.json',
        help='uri of a gzipped metadata json file'
    )
    parser.add_argument(
        '--metadata-file',
        type=str,
        default='data/metadata/metadata_sample.json',
        help='path to a metadata json file'
    )

    parser.add_argument(
        '--clickhouse-server',
        type=str,
        default='localhost:9000',
        help='clickhouse server address'
    )
    parser.add_argument(
        '--clickhouse-db',
        type=str,
        default='default',
        help='clickhouse server database'
    )
    parser.add_argument(
        '--clickhouse-user',
        type=str,
        default='default',
        help='clickhouse server user'
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
        '--metadata-table-name',
        type=str,
        default='metadata',
        help='clickhouse table name of metadata'
    )

    args, _ = parser.parse_known_args()
    logging.info(f'args: {args}')

    main(args)
