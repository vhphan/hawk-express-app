select * from hourly_stats.dc_e_nr_events_nrcellcu_flex_raw order by random() limit 10;

select max(date_id) from hourly_stats.dc_e_nr_events_nrcellcu_flex_raw;


select * from pg_indexes where tablename like 'clusters_kpi%'



select * from pg_indexes where schemaname = 'hourly_stats' 
and
tablename like 'clusters_kpi%'








