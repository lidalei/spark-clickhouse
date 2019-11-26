import argparse

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def main(args: argparse.Namespace):

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[*]", "StoreItem")
    ssc = StreamingContext(sc, 1)

    ssc.socketTextStream()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--test',
        type=str,
        default='Test',
        help='Test'
    )

    args, _ = parser.parse_known_args()
    print(args)

    main(args)
