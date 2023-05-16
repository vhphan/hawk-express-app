import os
from pyscript.db.main import postgres_db


def main():
    db = postgres_db('daily_stats', 'dnb')
    current_folder = os.path.dirname(os.path.abspath(__file__))
    db.run_sql_script_per_query(f'{current_folder}/refresh_views_daily.sql')


def main_hourly():
    db = postgres_db('hourly_stats', 'dnb')
    current_folder = os.path.dirname(os.path.abspath(__file__))
    db.run_sql_script_per_query(f'{current_folder}/refresh_views_hourly.sql')


def adhoc():
    db = postgres_db('daily_stats', 'dnb')
    current_folder = os.path.dirname(os.path.abspath(__file__))
    db.run_sql_script_per_query(f'{current_folder}/adhoc.sql')


if __name__ == '__main__':
    main()
    main_hourly()
