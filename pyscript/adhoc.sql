create materialized view hourly_stats.obs as
select t1.site_id,
t1.on_board_date::timestamp,
t2.datetime_actual as time
from dnb.daily_stats.df_dpm as t1
left join dnb.hourly_stats.d_hour as t2 on t2.date_actual >= t1.on_board_date::date
order by t1.site_id, t2.date_actual, t2.datetime_actual;