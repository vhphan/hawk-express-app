SELECT * FROM information_schema.tables limit 5;

SELECT * FROM dnb.information_schema.tables 
WHERE table_name ilike '%nrcell%'

SELECT * FROM dnb.information_schema.tables 
WHERE table_schema ilike '%stats%'

SELECT count(*) from dnb.daily_stats.dc_e_nr_nrcelldu_day;

show data_directory;

SELECT oid,* from pg_database

select pg_database.datname,pg_database.oid from pg_database;

SELECT count(*) from dnb.daily_stats.dc_e_nr_nrcelldu_v_day;

SELECT * from dnb.daily_stats.dc_e_nr_nrcelldu_day limit 5;

SELECT * from dnb.daily_stats.dc_e_nr_nrcelldu_v_day limit 5;

SELECT * FROM information_schema.table_constraints 
WHERE table_name = 'dc_e_nr_nrcelldu_day' and table_schema='public';

-- check if any unique index is in table

SELECT * FROM information_schema.table_constraints 
WHERE table_name = 'dc_e_nr_nrcelldu_day' and table_schema='daily_stats';

SELECT * FROM information_schema.columns 
WHERE column_name='dl_16qam_nom';

SELECT * FROM information_schema.columns 
WHERE column_name ilike 'dl_16qam%';

