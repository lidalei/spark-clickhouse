import logging
import typing

from retry import retry
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException, LogicalError, NetworkError, SocketTimeoutError


class ClickhouseClient(object):
    def __init__(self, host, logfmt, loglvl=logging.INFO):
        self.host = host
        # set logging format and level in driver / worker
        logging.basicConfig(format=logfmt)
        logging.getLogger().setLevel(loglvl)

        self.cli = Client.from_url(host)
        # perform a test
        self.test()

    @retry((NetworkError, ConnectionResetError, BrokenPipeError,), tries=5, delay=3, backoff=2, jitter=1, max_delay=10)
    def test(self):
        res = self.cli.execute('SELECT 1 + 1')
        if res[0][0] != 2:
            raise LogicalError(f'1 + 1 = {res[0][0]}')

    def execute_sql(self, sql: str):
        return self.cli.execute(sql)

    def tables_nonexist(self, tables: typing.List[str]) -> typing.List:
        """return non-existent tables in given database"""
        res = self.cli.execute('SHOW TABLES')
        db_tables = [t[0] for t in res]
        missing_tables = []
        for table in tables:
            if table not in db_tables:
                logging.error(f'table {table} does not exist')
                missing_tables.append(table)
        return missing_tables

    @retry((ServerException, SocketTimeoutError, NetworkError,), tries=10, delay=5, backoff=2, jitter=1, max_delay=60)
    def insert_partition(self, insert_sql: str, iterator: map):
        # convert iterator to list in order to retry in case of exceptions
        n = self.cli.execute(insert_sql, list(iterator))
        logging.info(f'inserted {n} rows')
