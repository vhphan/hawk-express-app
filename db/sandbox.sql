
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

