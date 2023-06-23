drop materialized view if exists daily_stats.kpi_nrcellcu_flex;
create materialized view daily_stats.kpi_nrcellcu_flex as
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
100 * sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
100 * sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
100 * sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
100 * sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g",
100 * sum("endc_sr_nom")|||sum("endc_sr_den")  as  "endc_sr" ,
100 * sum("erab_drop_nom")|||sum("erab_drop_den")  as  "erab_drop" ,
sum(max_rrc_connected_user_endc) as max_rrc_connected_user_endc,
sum(eps_fallback_attempt) as eps_fallback_attempt
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_day';

create index on dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day (flex_filtername);

create index on dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day (nrcellcu);

create unique index on dnb.daily_stats.kpi_nrcellcu_flex (date_id, region, mobile_operator);

select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 5;

/*markdown

drop materialized view if exists daily_stats.kpi_nrcelldu_flex;

select count(*) from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day;

drop materialized view if exists daily_stats.kpi_nrcelldu_flex;
create materialized view daily_stats.kpi_nrcelldu_flex as
with dt as (
    select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcelldu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
SELECT
date_id,
"Region" as region,
mobile_operator,
sum("ul_traffic_volume_nom")|||(1024*1024*1024)  as  "ul_traffic_volume" ,
sum("dl_traffic_volume_nom")|||(1024*1024*1024)  as  "dl_traffic_volume" ,
100 * sum("dl_qpsk_nom")|||sum("dl_modulation_den")  as  "dl_qpsk"  ,
100 * sum("dl_16qam_nom")|||sum("dl_modulation_den")  as  "dl_16qam"  ,
100 * sum("dl_64qam_nom")|||sum("dl_modulation_den")  as  "dl_64qam"  ,
100 * sum("dl_256qam_nom")|||sum("dl_modulation_den")  as  "dl_256qam"  ,
100 * sum("ul_qpsk_nom")|||sum("ul_modulation_den")  as  "ul_qpsk"  ,
100 * sum("ul_16qam_nom")|||sum("ul_modulation_den")  as  "ul_16qam"  ,
100 * sum("ul_64qam_nom")|||sum("ul_modulation_den")  as  "ul_64qam"  ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

select * from pg_indexes where tablename = 'dc_e_nr_events_nrcelldu_flex_day';

create index on dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day (flex_filtername);

create index on dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day (nrcelldu);

create unique index on dnb.daily_stats.kpi_nrcelldu_flex (date_id, region, mobile_operator);

drop materialized view if exists daily_stats.kpi_eutrancellfdd_flex;
create materialized view daily_stats.kpi_eutrancellfdd_flex as
with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_flex_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_eutrancellfdd2 = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
SELECT
date_id,
"Region" as region,
mobile_operator,
100 * sum("e-rab_setup_success_rate_nom")|||sum("e-rab_setup_success_rate_den")  as  "e-rab_setup_success_rate" ,
100 * sum("erab_drop_call_rate_nom")|||sum("erab_drop_call_rate_den")  as  "erab_drop_call_rate" ,
100 * sum("intrafreq_hosr_nom")|||sum("intrafreq_hosr_den")  as  "intrafreq_hosr" ,
100 * sum("packet_loss_(dl)_nom")|||sum("packet_loss_(dl)_den")  as  "packet_loss_(dl)" ,
100 * sum("packet_loss_(ul)_nom")|||sum("packet_loss_(ul)_den")  as  "packet_loss_(ul)" ,
100 * sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler" ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_nom")|||(1024*1024*1024)  as  "dl_data_volume" ,
sum("ul_data_volume_nom")|||(1024*1024*1024)  as  "ul_data_volume" , 
100 * sum("ul_qpsk_nom") ||| sum("ul_modulation_den")  as  "ul_qpsk"  , 
100 * sum("ul_16qam_nom") ||| sum("ul_modulation_den") as  "ul_16qam"  , 
100 * sum("ul_64qam_nom") ||| sum("ul_modulation_den") as  "ul_64qam"  , 
100 * sum("ul_256qam_nom") ||| sum("ul_modulation_den") as  "ul_256qam"  , 
100 * sum("dl_qpsk_nom") ||| sum("dl_modulation_den") as  "dl_qpsk"  , 
100 * sum("dl_16qam_nom") ||| sum("dl_modulation_den") as  "dl_16qam"  , 
100 * sum("dl_64qam_nom") ||| sum("dl_modulation_den") as  "dl_64qam"  , 
100 * sum("dl_256qam_nom") ||| sum("dl_modulation_den") as  "dl_256qam" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_flex_day 
where flex_filtername ilike '%99'
order by random() limit 5;

select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_flex_day 
where flex_filtername not ilike '%99'
order by random() limit 5;

select * from pg_indexes where tablename = 'dc_e_erbs_eutrancellfdd_flex_day';

create index on dnb.daily_stats.dc_e_erbs_eutrancellfdd_flex_day (eutrancellfdd);
create index on dnb.daily_stats.dc_e_erbs_eutrancellfdd_flex_day (flex_filtername);


create unique index on dnb.daily_stats.kpi_eutrancellfdd_flex (date_id, region, mobile_operator);

refresh materialized view concurrently daily_stats.kpi_eutrancellfdd_flex;
refresh materialized view concurrently daily_stats.kpi_nrcellcu_flex;
refresh materialized view concurrently daily_stats.kpi_nrcelldu_flex;



select * from daily_stats.kpi_eutrancellfdd_flex limit 5;







select * from information_schema.tables where table_schema = 'daily_stats' order by table_name;

select ul_modulation_den from daily_stats.dc_e_erbs_eutrancellfdd_day order by random() limit 1000;









-- select active process
select * from pg_stat_activity where state = 'active';

select pid, pg_blocking_pids(pid) as blocked_by, query as blocked_query
from pg_stat_activity
where pg_blocking_pids(pid)::text != '{}';

select * from pg_stat_activity where pid=2459672;

-- terminate process with pid

select pg_terminate_backend(2459672);

VACUUM (VERBOSE, ANALYZE) daily_stats.dc_e_nr_events_nrcelldu_flex_day;