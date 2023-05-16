drop materialized view if exists hourly_stats.kpi_nr_nrcelldu_2;
create materialized view hourly_stats.kpi_nr_nrcelldu_2 as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_nrcelldu_raw as t1
        INNER JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.daily_stats.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
select
date_id,
"Region" as region,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_gb_nom")|||(1024*1024*1024)  as  "dl_data_volume_gb" ,
sum("ul_data_volume_gb_nom")|||(1024*1024*1024)  as  "ul_data_volume_gb" ,
sum("total_traffic_gb_nom")|||(1024*1024*1024)  as  "total_traffic_gb" ,
sum("dl_qpsk_nom")  as  "dl_qpsk"  ,
sum("dl_16qam_nom")  as  "dl_16qam"  ,
sum("dl_64qam_nom")  as  "dl_64qam"  ,
sum("dl_256qam_nom")  as  "dl_256qam"  ,
sum("ul_qpsk_nom")  as  "ul_qpsk"  ,
sum("ul_16qam_nom")  as  "ul_16qam"  ,
sum("ul_64qam_nom")  as  "ul_64qam"  ,
sum("ul_256qam_nom")  as  "ul_256qam"  ,
sum("dl_mac_vol_to_scell_nom")|||sum("dl_mac_vol_to_scell_den")  as  "dl_mac_vol_to_scell" ,
sum("dl_mac_vol_as_scell_nom")|||sum("dl_mac_vol_as_scell_den")  as  "dl_mac_vol_as_scell" ,
sum("dl_mac_vol_to_scell_ext_nom")|||sum("dl_mac_vol_to_scell_ext_den")  as  "dl_mac_vol_to_scell_ext" ,
sum("dl_mac_vol_as_scell_ext_nom")|||sum("dl_mac_vol_as_scell_ext_den")  as  "dl_mac_vol_as_scell_ext" ,
sum("cell_availability_nom")|||sum("cell_availability_den")  as  "cell_availability" ,
sum("resource_block_utilizing_rate_dl_nom")|||sum("resource_block_utilizing_rate_dl_den")  as  "resource_block_utilizing_rate_dl" ,
sum("resource_block_utilizing_rate_ul_nom")|||sum("resource_block_utilizing_rate_ul_den")  as  "resource_block_utilizing_rate_ul" ,
sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler"
from
dt
    where "Region" is not null
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id;