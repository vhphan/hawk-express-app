--UPDATE dnb.hourly_stats.dc_e_nr_events_nrcellcu_flex_raw
--SET date_id = date_id + INTERVAL '14 days';

SELECT DISTINCT date_id FROM hourly_stats.dc_e_erbs_eutrancellfdd_flex_raw;

SELECT DISTINCT date_id FROM hourly_stats.dc_e_nr_events_nrcellcu_flex_raw;


DELETE FROM hourly_stats.dc_e_nr_events_nrcellcu_flex_raw
WHERE date_id <= '2023-06-24';

SELECT DISTINCT date_id from hourly_stats.dc_e_nr_events_nrcellcu_flex_raw order by date_id desc;

SELECT date_id, COUNT(*) AS COUNT
FROM hourly_stats.dc_e_nr_events_nrcellcu_flex_raw
GROUP BY date_id;

--nrcellcu_flex
drop materialized view if exists hourly_stats.clusters_kpi_nrcellcu_flex;
create materialized view hourly_stats.clusters_kpi_nrcellcu_flex as
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
"Cluster_ID" as cluster_id,
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
where "Cluster_ID" is not null
group by date_id, mobile_operator, cluster_id
order by cluster_id, date_id;



--nrcellcu_flex
create unique index on hourly_stats.clusters_kpi_nrcellcu_flex(date_id, cluster_id, mobile_operator);

SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_raw';

refresh materialized view concurrently hourly_stats.clusters_kpi_nrcellcu_flex;

SELECT DISTINCT date_id FROM hourly_stats.dc_e_nr_events_nrcelldu_flex_raw ORDER BY date_id DESC;

DELETE FROM hourly_stats.dc_e_nr_events_nrcelldu_flex_raw
WHERE date_id > '2023-07-09';

SELECT date_id, COUNT(*) AS COUNT
FROM hourly_stats.dc_e_nr_events_nrcelldu_flex_raw
GROUP BY date_id;

--nrcelldu_flex
drop materialized view if exists hourly_stats.clusters_kpi_nrcelldu_flex;
create materialized view hourly_stats.clusters_kpi_nrcelldu_flex as
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
"Cluster_ID" as cluster_id,
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
group by date_id, mobile_operator, cluster_id
order by cluster_id, date_id;

--nrcelldu_flex
create unique index on hourly_stats.clusters_kpi_nrcelldu_flex(date_id, cluster_id, mobile_operator);

--drop materialized view if exists hourly_stats.clusters_kpi_nrcellfdd_flex;

refresh materialized view concurrently hourly_stats.clusters_kpi_nrcelldu_flex;

-- refresh materialized view concurrently hourly_stats.kpi_eutrancellfdd_flex;

