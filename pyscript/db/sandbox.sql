select * from information_schema.columns where column_name ilike '%modulation%'
and table_schema = 'daily_stats';

select * from information_schema.tables where table_name ilike '%_v2';

select 	ul_user_throughput_den from daily_stats.dc_e_erbs_eutrancellfdd_day order by random() limit 10;







