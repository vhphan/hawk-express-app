from loguru import logger
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors
from pyscript.db.main import postgres_db

with postgres_db('hourly_stats', 'dnb') as db:

    days = db.query(
        'select distinct date_id from hourly_stats.dc_e_nr_events_nrcelldu_flex_raw_v2 order by date_id', return_dict=True)

    for day in days:
        date_id = day['date_id']
        print(date_id)
        sql = f"""
        insert into hourly_stats.dc_e_nr_events_nrcelldu_flex_raw
        (
            date_id,
            nr_name,
            nrcelldu,
            flex_filtername,
            ul_modulation_den,
            ul_16qam_nom,
            ul_64qam_nom,
            dl_user_throughput_nom,
            dl_user_throughput_den,
            ul_user_throughput_nom,
            ul_user_throughput_den,
            ul_traffic_volume_nom,
            ul_traffic_volume_den,
            dl_traffic_volume_nom,
            dl_traffic_volume_den,
            dl_qpsk_nom,
            dl_16qam_nom,
            dl_64qam_nom,
            dl_256qam_nom,
            dl_modulation_den,
            ul_qpsk_nom
        )
        select
            date_id,
            nr_name,
            nrcelldu,
            flex_filtername,
            ul_modulation_den,
            ul_16qam_nom,
            ul_64qam_nom,
            dl_user_throughput_nom,
            dl_user_throughput_den,
            ul_user_throughput_nom,
            ul_user_throughput_den,
            ul_traffic_volume_nom,
            ul_traffic_volume_den,
            dl_traffic_volume_nom,
            dl_traffic_volume_den,
            dl_qpsk_nom,
            dl_16qam_nom,
            dl_64qam_nom,
            dl_256qam_nom,
            dl_modulation_den,
            ul_qpsk_nom
            from hourly_stats.dc_e_nr_events_nrcelldu_flex_raw_v2
            where date_id = '{date_id}'
            """
        try:
            db.execute_and_commit(sql)
            logger.info(f'inserted {date_id}')
        except errors.lookup(UNIQUE_VIOLATION) as e:
            logger.error(e)