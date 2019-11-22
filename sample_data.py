import gzip
import argparse


def parse_gzip(path):
    with gzip.open(path, 'r') as g:
        for l in g:
            yield l


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--filename',
        help='gzip file',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--out',
        help='output file',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--size',
        type=int,
        default=10000,
    )

    args, _ = parser.parse_known_args()
    with open(args.out, 'wb') as w:
        lino = 0
        for l in parse_gzip(args.filename):
            w.write(l)
            lino += 1
            if lino >= args.size:
                break
