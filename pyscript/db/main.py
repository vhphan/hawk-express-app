
import os
import time
from datetime import datetime, date
from pathlib import Path
from sys import platform
from typing import Union

import pandas as pd
import psycopg2
from dotenv import dotenv_values
from loguru import logger
from psycopg2 import sql, ProgrammingError
from psycopg2.errors import InFailedSqlTransaction, OperationalError
from retry import retry
from sqlalchemy import text, create_engine, inspect
from dotenv import load_dotenv

load_dotenv()

class Singleton:

    def __init__(self, cls):
        self._cls = cls

    def Instance(self, **kwargs):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._cls(**kwargs)
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)


class EPDB:

    def __init__(self, dbc, db_type='mysql', schema=None):
        self.dbc = dbc
        self.db_type = db_type
        self.schema = schema

        try:
            if db_type == 'postgres':
                con_string = f"postgresql://{dbc['user']}:{(dbc['password'])}@{dbc['host']}:{dbc['port']}/{dbc['dbname']}"

                if schema is not None:
                    dbc['options'] = f"-c search_path={schema},public"
                    con_string += f'?options=-csearch_path%3D{schema},public'

                self.engine = create_engine(con_string, echo=False, pool_pre_ping=True)
                self._conn = psycopg2.connect(**dbc)
                self._cursor = self._conn.cursor()

        except Exception as e:
            logger.error(e.__repr__())

    def __repr__(self):
        return f'{self.dbc}, {self.schema}, {self.db_type}'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    def __str__(self):
        return repr(self._conn)

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def connect(self):
        self._conn = psycopg2.connect(**self.dbc)
        self._cursor = self._conn.cursor()

    @retry(tries=5, delay=10, backoff=2)
    def execute_and_commit(self, sql, params=None):
        try:
            self.execute(sql, params or ())
            self.commit()
        except Exception as e:
            self.connection.close()
            self.connect()
            raise e

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    @property
    def description(self):
        return self.cursor.description

    def query(self, sql, params=None, return_dict=False) -> Union[list[dict], list[tuple], None]:
        try:
            if self._conn is None:
                self.__init__(dbc=self.dbc, db_type=self.db_type, schema=self.schema)
            elif self.db_type == 'mysql':
                self._conn.ping(True)
            self.cursor.execute(sql, params or ())
            # self.cursor.rowcount gives number of rows affected
        except Exception as e:
            logger.debug(f'{e} : {sql}')
            raise e

        if self.cursor.description is None:  # is None if no rows returned, fetchall will raise exception
            if not self.cursor.connection.autocommit:
                self.commit()
            return
        if return_dict:
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        return self.cursor.fetchall()

    @retry(OperationalError, tries=3, delay=10, logger=logger)
    def query_df(self, sql, **kwargs) -> pd.DataFrame:
        return pd.read_sql(text(sql), self.engine.connect(), **kwargs)

    def vacuum_table(self, table_name):
        logger.info(f'Vacuuming table {self.schema}.{table_name}')
        old_isolation_level = self.connection.isolation_level
        self.connection.set_isolation_level(0)
        self.execute(f'VACUUM ANALYZE {self.schema}."{table_name}"')
        self.connection.set_isolation_level(old_isolation_level)
        logger.info(f'Finished vacuuming table {self.schema}.{table_name}')

    @retry(OperationalError, tries=3, delay=10, logger=logger)
    def df_to_db(self, df, **kwargs):
        try:
            df.to_sql(**kwargs, con=self.engine)
        except Exception as e:
            df.to_csv(f"/home2/eproject/dnb/debug/csv/{kwargs['name']}", index=False)
            raise e

    def df_to_db_replace(self, df: pd.DataFrame, name: str, unique_keys: list[str], **kwargs):
        try:
            df_existing = self.query_df(f'SELECT * FROM {self.schema}.{name}')
        except Exception as e:
            logger.debug(e)
            logger.info(f'Table {self.schema}.{name} does not exist. Creating table.')
            self.df_to_db(df, name=name, index=False, **kwargs)
            return
        df_new = pd.concat([df_existing, df])
        df_new.drop_duplicates(subset=unique_keys, inplace=True, keep='last')
        self.empty_table(name)
        df_new.to_sql(name, self.engine, if_exists='append', index=False)

    def empty_table(self, table_name):

        insp = inspect(self.engine)
        if not insp.has_table(table_name):
            logger.info(f'{table_name} does not exist. Nothing to empty/delete')
            return

        logger.info(f'trying to empty {table_name}')
        cursor = self.cursor

        stmt = sql.SQL(f"""
            DELETE FROM {self.schema}."{table_name}" WHERE TRUE
        """).format(
            schema=sql.Identifier(self.schema),
            table_name=sql.Identifier(table_name),
        )
        cursor.execute(stmt)
        self.commit()

        rowcount = cursor.rowcount
        return rowcount

    def remove_duplicates_in_table(self, table_name, unique_columns):
        logger.info(f'trying to remove duplicates in {table_name}')
        with self._cursor as cursor:
            pass

    def run_sql_script_per_query(self, file_path: str):
        with open(file_path, 'r') as fp:
            file_content = fp.read()
        sqls = file_content.split(';')
        for i, sql in enumerate(sqls):
            if len(sql.strip()) > 0 and not sql.strip().startswith('--'):
                start = time.perf_counter()
                logger.info(f'running sql #{i + 1}/{len(sqls)}: {sql[0:150]}')
                if i + 1 < len(sqls):
                    logger.info(f'next query is {sqls[i + 1][0:150]}')
                if len(sql) > 0:
                    try:
                        sql = sql.replace('%', '%%')
                        self.execute_and_commit(sql)
                    except Exception as e:
                        logger.error(e)
                        logger.debug('Skipping the following sql')
                        logger.debug(f'\n{sql}')

                    end = time.perf_counter()
                    time_taken = end - start
                    logger.info(f'{sql[0:150]} took {time_taken} seconds')
                    logger.info('executed')

    def rollback_transaction(self):
        self._conn.rollback()

    @property
    def version(self):
        if self.db_type == 'postgres':
            return self.query('SELECT VERSION()')[0][0]

def get_db_config():
    return dict(
        host='localhost',
        port=5432,
        user=os.getenv('PGDB_USER'),
        password=os.getenv('PGDB_PASSWORD'),
    )

def postgres_db(schema, db_name) -> EPDB:
    dbc = dict(dbname=db_name, **get_db_config())
    print(f'schema is {schema}', f'port is {dbc["port"]}', f'database is {db_name}')

    return EPDB(dbc=dbc, db_type='postgres', schema=schema)

if __name__ == '__main__':
    db = postgres_db('dnb', 'dnb')
