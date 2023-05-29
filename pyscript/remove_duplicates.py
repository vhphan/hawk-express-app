import pandas as pd
from loguru import logger
from pyscript.db.main import postgres_db
logger.add("logs/remove_duplicates.log", rotation="5 MB")

tables = [
    # 'dc_e_nr_nrcellcu_day',
    # 'dc_e_nr_events_nrcellcu_flex_day',
    # 'dc_e_nr_nrcelldu_day',
    # 'dc_e_nr_events_nrcelldu_flex_day',
    'dc_e_vpp_rpuserplanelink_v_day',
    'dc_e_erbsg2_mpprocessingresource_v_day',
    'dc_e_nr_nrcelldu_v_day',
]
unique_cols = [
    # ['date_id', 'nrcellcu'],
    # ['date_id', 'nrcellcu', 'flex_filtername'],
    # ['date_id', 'nrcelldu'],
    # ['date_id', 'nrcelldu', 'flex_filtername'],
    ['date_id', 'ne_name'],
    ['date_id', 'erbs'],
    ['date_id', 'nrcelldu'],
]


def remove_duplicates(dataframe: pd.DataFrame, unique_keys: list[str]):
    """
    Remove duplicates from the input dataframe based on unique keys.

    :param dataframe: The input dataframe to remove duplicates from.
    :type dataframe: pandas.DataFrame
    :param unique_keys: The unique keys to consider when removing duplicates.
    :type unique_keys: List[str]
    :return: A copy of the input dataframe without duplicates.
    :rtype: pandas.DataFrame
    """
    return (
        dataframe.copy()
        .drop_duplicates(subset=unique_keys, keep='last')
    )


def main(schema='daily_stats', db_name='dnb'):
    for i, table in enumerate(tables):
        sql_dates = f'select distinct date_id from {schema}.{table} order by date_id desc;'
        with postgres_db(schema, db_name) as db:
            dates = db.query(sql_dates, return_dict=True)

        for d in dates:
            with postgres_db(schema, db_name) as db:
                date_str = d['date_id'].strftime('%Y-%m-%d')
                df = db.query_df(f'select * from {schema}.{table} where date_id = \'{date_str}\'')
                init_rows = df.shape[0]
                df = (
                    df.pipe(remove_duplicates, unique_keys=unique_cols[i])
                )
                final_rows = df.shape[0]
                if init_rows == final_rows:
                    logger.info(f'no duplicates in {table} {date_str}')
                if init_rows > final_rows:
                    logger.info(f'{table} {date_str} {init_rows} {final_rows}')
                    db.query(
                        f'delete from {schema}.{table} where date_id = \'{date_str}\'')
                    try:
                        db.df_to_db(df, name=table, index=False, if_exists='append')
                    except Exception as e:
                        logger.error(e)
                        logger.info(f'failed {table} {date_str}')
                        df.to_csv(f'logs/insert_failed_{table}_{date_str}.csv', index=False)
                        raise e
                    logger.info(f'completed {table} {date_str}')


if __name__ == '__main__':
    main()
