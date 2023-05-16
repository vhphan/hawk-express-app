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
sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_day';

create index on dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day (flex_filtername);

create index on dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day (nrcellcu);

create unique index on dnb.daily_stats.kpi_nrcellcu_flex (date_id, region, mobile_operator);

select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 5;

/*markdown

*/

drop materialized view if exists daily_stats.kpi_nrcelldu_flex cascade;
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
sum("dl_qpsk_nom")  as  "dl_qpsk"  ,
sum("dl_16qam_nom")  as  "dl_16qam"  ,
sum("dl_64qam_nom")  as  "dl_64qam"  ,
sum("dl_256qam_nom")  as  "dl_256qam"  ,
sum("ul_qpsk_nom")  as  "ul_qpsk"  ,
sum("ul_16qam_nom")  as  "ul_16qam"  ,
sum("ul_64qam_nom")  as  "ul_64qam"  ,
sum("dl_user_throughput_nom")|||1000  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||1000  as  "ul_user_throughput" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

select * from pg_indexes where tablename = 'dc_e_nr_events_nrcelldu_flex_day';

create index on dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day (flex_filtername);

create index on dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day (nrcelldu);

create unique index on dnb.daily_stats.kpi_nrcelldu_flex (date_id, region, mobile_operator);

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
sum("ul_qpsk_nom")  as  "ul_qpsk"  ,
sum("ul_16qam_nom")  as  "ul_16qam"  ,
sum("ul_64qam_nom")  as  "ul_64qam"  ,
sum("ul_256qam_nom")  as  "ul_256qam"  ,
sum("packet_loss_(dl)_nom")|||sum("packet_loss_(dl)_den")  as  "packet_loss_(dl)" ,
sum("packet_loss_(ul)_nom")|||sum("packet_loss_(ul)_den")  as  "packet_loss_(ul)" ,
sum("e-rab_setup_success_rate_nom")|||sum("e-rab_setup_success_rate_den")  as  "e-rab_setup_success_rate" ,
sum("erab_drop_call_rate_nom")|||sum("erab_drop_call_rate_den")  as  "erab_drop_call_rate" ,
sum("intrafreq_hosr_nom")|||sum("intrafreq_hosr_den")  as  "intrafreq_hosr" ,
sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler" ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_nom")|||(1024*1024*1024)  as  "dl_data_volume" ,
sum("ul_data_volume_nom")|||(1024*1024*1024)  as  "ul_data_volume" ,
sum("dl_qpsk_nom")  as  "dl_qpsk"  ,
sum("dl_16qam_nom")  as  "dl_16qam"  ,
sum("dl_64qam_nom")  as  "dl_64qam"  ,
sum("dl_256qam_nom")  as  "dl_256qam" 
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

