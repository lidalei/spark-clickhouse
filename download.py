import logging
from pathlib import Path
import requests
import zlib
from retry import retry


def _get_file_size(path: str) -> int:
    """get file size, raise an exception if it is a dir, return 0 if it does not exist"""
    p = Path(path)
    if not p.exists():
        return 0

    if p.is_dir():
        raise IsADirectoryError(f'{path} is a directory')

    return p.stat().st_size


@retry((requests.RequestException,), tries=10, delay=5, backoff=2, jitter=1, max_delay=60)
def download_unzip_large_gz_file(uri: str, target_file: str, chunk_size=1024 * 1024):
    """
    download and unzip large remote file and save the unzipped content to target path
    :param uri: http or https uri
    :param target_file: path to target file
    :param chunk_size: chunk size in bytes to stream remote gzipped file through https / http
    """
    if uri == target_file:
        return

    res = requests.head(uri)
    target_file_size = res.headers['Content-Length']

    file_size = _get_file_size(target_file)
    # FIXME! Maybe download the gzipped file in the meantime to have an accurate check
    if target_file_size > file_size:
        logging.info(f'{target_file} exists and has {file_size} bytes, do nothing')
        return

    logging.info('existing file is smaller than remote one, gonna make a new request')

    # https://stackoverflow.com/a/12572031
    decoder = zlib.decompressobj(32 + zlib.MAX_WBITS)
    # Sadly it is a gzipped file. We cannot specify Range to skip existing bytes.
    # headers = {'Range': 'bytes=%d-' % file_size}
    r = requests.get(uri, stream=True)
    if r.status_code != 200:
        raise requests.ConnectionError(f'non 200 status code: {r.status_code}')

    with open(target_file, mode='wb') as f:
        for bs in r.iter_content(chunk_size):
            f.write(decoder.decompress(bs))
