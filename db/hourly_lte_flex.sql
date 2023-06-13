drop materialized view if exists hourly_stats.kpi_eutrancellfdd_flex;
create materialized view hourly_stats.kpi_eutrancellfdd_flex as
with dt as (
    select * from dnb.hourly_stats.dc_e_erbs_eutrancellfdd_flex_raw as t1
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


create unique index on dnb.hourly_stats.kpi_eutrancellfdd_flex (date_id, region, mobile_operator);

refresh materialized view concurrently dnb.hourly_stats.kpi_eutrancellfdd_flex;

refresh materialized view concurrently dnb.hourly_stats.kpi_nrcellcu_flex;
refresh materialized view concurrently dnb.hourly_stats.kpi_nrcelldu_flex;

