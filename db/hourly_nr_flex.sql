drop materialized view if exists hourly_stats.kpi_nrcellcu_flex;

create materialized view hourly_stats.kpi_nrcellcu_flex as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_flex_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcellcu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcellcu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'    
)
SELECT
date_id,
"Region" as region,
mobile_operator,
100 * sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
100 * sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
100 * sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
100 * sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

create unique index on hourly_stats.kpi_nrcelldu_flex(date_id, region, mobile_operator);

SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_raw';

drop materialized view if exists hourly_stats.kpi_nrcelldu_flex;
create materialized view hourly_stats.kpi_nrcelldu_flex as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_events_nrcelldu_flex_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcelldu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
SELECT
date_id,
"Region" as region,
mobile_operator,
sum("ul_traffic_volume_nom")|||(1024*1024*1024)  as  "ul_traffic_volume" ,
100 * sum("dl_qpsk_nom")|||sum("dl_modulation_den")  as  "dl_qpsk"  ,
100 * sum("dl_16qam_nom")|||sum("dl_modulation_den")  as  "dl_16qam"  ,
100 * sum("dl_64qam_nom")|||sum("dl_modulation_den")  as  "dl_64qam"  ,
100 * sum("dl_256qam_nom")|||sum("dl_modulation_den")  as  "dl_256qam"  ,
100 * sum("ul_qpsk_nom")|||sum("ul_modulation_den")  as  "ul_qpsk"  ,
100 * sum("ul_16qam_nom")|||sum("ul_modulation_den")  as  "ul_16qam"  ,
100 * sum("ul_64qam_nom")|||sum("ul_modulation_den")  as  "ul_64qam"  ,
sum("dl_user_throughput_nom")|||1000  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||1000  as  "ul_user_throughput" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

create unique index on hourly_stats.kpi_nrcellcu_flex(date_id, region, mobile_operator);

drop materialized view if exists hourly_stats.kpi_nrcellfdd_flex;


create unique index on hourly_stats.kpi_eutrancellfdd_flex(date_id, region, mobile_operator);

refresh materialized view concurrently hourly_stats.kpi_nrcelldu_flex;
refresh materialized view concurrently hourly_stats.kpi_nrcellcu_flex;
-- refresh materialized view concurrently hourly_stats.kpi_eutrancellfdd_flex;

select * from pg_indexes where tablename = 'kpi_nrcelldu_flex';



