
create table rfdb.flex_filters
(
    mobile_operator                varchar(50),
    flex_filtername_nrcelldu       varchar(50),
    flex_filtername_nrcellcu       varchar(50),
    flex_filtername_eutrancellfdd  varchar(50),
    flex_filtername_eutrancellfdd2 varchar(50)
);


create index flex_filters_mobile_operator_index
    on rfdb.flex_filters (mobile_operator);


INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('Maxis', 'mcc502mnc12', 'UeGrpId2', 'Plmn1Endc2To99', 'Plmn1');
INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('Umobile', 'mcc502mnc18', 'UeGrpId5', 'Plmn4Endc2To99', 'Plmn4');
INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('YTL', 'mcc502mnc152', 'UeGrpId1', 'Plmn0Endc2To99', 'Plmn0');
INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('Digi', 'mcc502mnc16', 'UeGrpId6', 'Plmn5Endc2To99', 'Plmn5');
INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('Celcom', 'mcc502mnc19', 'UeGrpId3', 'Plmn2Endc2To99', 'Plmn2');
INSERT INTO rfdb.flex_filters (mobile_operator, flex_filtername_nrcelldu, flex_filtername_nrcellcu, flex_filtername_eutrancellfdd, flex_filtername_eutrancellfdd2) VALUES ('TM', 'mcc502mnc153', 'UeGrpId4', 'Plmn3Endc2To99', 'Plmn3');



select flex_filtername, count(*) from daily_stats.dc_e_nr_events_nrcelldu_flex_day
group by flex_filtername;



select * from daily_stats.dc_e_nr_events_nrcellcu_flex_day
where flex_filtername ilike 'mcc%'
order by random() limit 5;




select date_id, dl_data_volume_gb
 from hourly_stats.kpi_nr_nrcelldu order by dl_data_volume_gb desc limit 5;







select * from pg_indexes where tablename = 'dc_e_nr_events_nrcellcu_flex_day';

select * from pg_indexes where tablename = 'dc_e_erbs_eutrancellfdd_flex_day';

select * from pg_indexes where indexdef like '%UNIQUE%' and tablename ilike 'dc_e%'
and schemaname='daily_stats';
;

select * from pg_indexes where indexdef like '%UNIQUE%' and tablename ilike 'dc_e%'
and schemaname='hourly_stats';

select * from information_schema.tables
where
table_schema='hourly_stats' and table_name ilike 'dc_e%';
;





select * from dnb.daily_stats.kpi_nrcelldu_flex as dt order by date_id limit 5;

select mobile_operator from dnb.rfdb.flex_filters group by mobile_operator;







select 
"5g_ho_success_rate_dnb_5g_to_dnb_nom",
"5g_ho_success_rate_dnb_5g_to_dnb_den",
"inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom",
"inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den"
from dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day as dt order by random() limit 5;





select * from information_schema.tables where  table_name ilike '%_v2';

select t1.column_name as col1, t2.column_name as col2
from information_schema.columns t1 
left join information_schema.columns t2 
on t1.column_name = t2.column_name
and t1.table_schema = t2.table_schema
where t1.table_name = 'dc_e_nr_events_nrcelldu_flex_day_v2' and t2.table_name = 'dc_e_nr_events_nrcelldu_flex_day'
and t1.table_schema = 'daily_stats' and t2.table_schema = 'daily_stats'
and t1.column_name ilike '%traf%'
;


select * from information_schema.columns where table_name = 'dc_e_nr_events_nrcelldu_flex_day_v2' and table_schema = 'daily_stats' and column_name ilike '%traf%';

-- add column dl_traffic_volume_nom to dc_e_nr_events_nrcelldu_flex_day

alter table hourly_stats.dc_e_nr_events_nrcelldu_flex_raw
add column dl_traffic_volume_den double precision;

-- get unique index for table dc_e_nr_events_nrcelldu_flex_day

select * from pg_indexes where tablename ilike 'dc_e_nr_events_nrcelldu_flex_%' and indexdef like '%UNIQUE%';

select 
t2.date_id, t2.flex_filtername, t2.nrcelldu, 
t2.dl_traffic_volume_nom, t2.dl_traffic_volume_den from hourly_stats.dc_e_nr_events_nrcelldu_flex_raw t1
inner join hourly_stats.dc_e_nr_events_nrcelldu_flex_raw_v2 t2
on t1.date_id = t2.date_id and t1.flex_filtername = t2.flex_filtername
and t1.nrcelldu = t2.nrcelldu
; 

select * from daily_stats.dc_e_nr_events_nrcelldu_flex_day_v2
limit 10;

select count(*) from hourly_stats.dc_e_nr_events_nrcelldu_flex_raw_v2;

select * from information_schema.columns where table_name = 'dc_e_nr_events_nrcelldu_flex_raw_v2' order by ordinal_position;

-- concatenate column_names from information_schema.columns for table_name dc_e_nr_events_nrcelldu_flex_raw_v2

select 
string_agg(column_name, ',') from information_schema.columns where table_name = 'dc_e_nr_events_nrcelldu_flex_raw_v2' 
group by table_name

;



insert into hourly_stats.dc_e_nr_events_nrcelldu_flex_day
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

-- get all processes

select * from pg_stat_activity 

where query ilike '%insert%' 
-- and query not ilike '%pg_stat_activity%'

order by backend_start desc;


select * from pg_stat_activity where state ilike 'idle in transaction%'
order by backend_start desc;


--  get all processes where backend_start is greater than 12 hours

-- select * from pg_stat_activity where query ilike '%LIMIT 1000%' and backend_start < now() - interval '12 hours' order by backend_start desc;

select pg_terminate_backend(pid) from pg_stat_activity where backend_start < now() - interval '12 hours'
and state ilike 'idle in transaction%'
order by backend_start desc;




-- kill all processes where backend_start is greater than 12 hours

select pg_terminate_backend(pid) from pg_stat_activity where query ilike '%LIMIT 1000%' and backend_start < now() - interval '12 hours' order by backend_start desc;



select max(date_id) from daily_stats.dc_e_nr_nrcellcu_day ;

select count(*) from rfdb.cell_mapping;

select min(date_id), max(date_id) from hourly_stats.kpi_nrcellcu_flex;

select min(date_id), max(date_id) from hourly_stats.kpi_nr_nrcelldu

select count(*) from hourly_stats.kpi_nrcelldu_flex









-- query to check location of postgres config file

SELECT name, setting FROM pg_settings WHERE name = 'config_file';

SHOW config_file;

SELECT max(date_id) from daily_stats.kpi_nr_nrcelldu;

select * from information_schema.columns where table_name ilike 'dc_e_nr_events_nrcelldu_flex_day' and table_schema = 'daily_stats';

select * from information_schema.columns where table_name ilike 'dc_e_erbs_eutrancellfdd_flex_day' and table_schema = 'daily_stats';

select "endc_sr_den " from daily_stats.dc_e_nr_events_nrcellcu_flex_day order by random() limit 5;

select "endc_sr_den" from daily_stats.dc_e_nr_events_nrcellcu_flex_day order by random() limit 5;

select * from information_schema.columns where column_name like '% ';

-- rename column that has a space at the end
with cte as (
select column_name from information_schema.columns where column_name like '% '
)
select 'alter table daily_stats.dc_e_nr_events_nrcellcu_flex_day rename column "' || column_name || '" to ' || trim(column_name) || ';' from cte;




alter table hourly_stats.dc_e_nr_events_nrcellcu_flex_raw rename column "endc_sr_nom " to endc_sr_nom;
alter table hourly_stats.dc_e_nr_events_nrcellcu_flex_raw rename column "endc_sr_den " to endc_sr_den;
alter table hourly_stats.dc_e_nr_events_nrcellcu_flex_raw rename column "erab_drop_nom " to erab_drop_nom;
alter table hourly_stats.dc_e_nr_events_nrcellcu_flex_raw rename column "erab_drop_den " to erab_drop_den;

select "endc_sr_nom " from daily_stats.dc_e_nr_events_nrcellcu_flex_day order by random() limit 5;



select "endc_sr_den " from daily_stats.dc_e_nr_events_nrcellcu_flex_day order by random() limit 5;



select count(*) from dnb.daily_stats.cells_list;

select distinct tech from dnb.daily_stats.cells_list limit 5;

select date_id, dl_user_throughput_den from dnb.daily_stats.dc_e_nr_nrcelldu_day
where dl_user_throughput_den<>1000
order by random() limit 5;
;

select count(*) from dnb.daily_stats.dc_e_nr_nrcelldu_day
where dl_user_throughput_den<>1000;

select * from pg_indexes where indexdef ilike '%unique%' and tablename ilike 'kpi%'
order by tablename;
;


drop index kpi_erbs_eutrancellfdd_v_date_id_region_idx1;

-- drop the following indexes
-- kpi_erbs_eutrancellfdd_v_date_id_region_idx1
-- kpi_erbs_eutrancellfdd_v_date_id_region_idx1
-- kpi_erbs_eutrancellrelation_date_id_region_idx1
-- kpi_erbs_eutrancellrelation_date_id_region_idx1
-- kpi_erbsg2_mpprocessingresource_v_date_id_region_idx1
-- kpi_erbsg2_mpprocessingresource_v_date_id_region_idx1
-- kpi_nr_nrcelldu_v_date_id_region_idx1
-- kpi_nr_nrcelldu_v_date_id_region_idx1

drop index hourly_stats.kpi_erbs_eutrancellfdd_v_date_id_region_idx1;
drop index hourly_stats.kpi_erbs_eutrancellrelation_date_id_region_idx1;
drop index hourly_stats.kpi_erbsg2_mpprocessingresource_v_date_id_region_idx1;
drop index hourly_stats.kpi_nr_nrcelldu_v_date_id_region_idx1;
    





select flex_filtername, max_rrc_connected_user_endc, eps_fallback_attempt from
daily_stats.dc_e_nr_events_nrcellcu_flex_day
order by random() LIMIT 10;

select date_id, sum(eps_fallback_attempt) from
daily_stats.dc_e_nr_events_nrcellcu_flex_day
group by date_id
order by random() LIMIT 10;


select * from dnb.rfdb.flex_filters;

with dt as (
    select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcellcu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcellcu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
SELECT
date_id,
"Region" as region,
mobile_operator,
sum(max_rrc_connected_user_endc) as max_rrc_connected_user_endc
-- sum(eps_fallback_attempt) as eps_fallback_attempt
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;



select distinct "Cluster_ID" from dnb.rfdb.cell_mapping;



-- get all materialized views in a schema

select matviewname from pg_matviews where schemaname = 'daily_stats';

