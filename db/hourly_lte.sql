drop materialized view if exists dnb.hourly_stats.kpi_erbs_eutrancellfdd;
create materialized view dnb.hourly_stats.kpi_erbs_eutrancellfdd as
with dt as (
    select * from dnb.hourly_stats.dc_e_erbs_eutrancellfdd_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select 
date_id,
"Region" as region,
sum("cell_availability_nom")|||sum("cell_availability_den")  as  "cell_availability" ,
sum("rrc_setup_success_rate_(service)_nom")|||sum("rrc_setup_success_rate_(service)_den")  as  "rrc_setup_success_rate_(service)" ,
sum("rrc_setup_success_rate_(signaling)_nom")|||sum("rrc_setup_success_rate_(signaling)_den")  as  "rrc_setup_success_rate_(signaling)" ,
sum("e-rab_setup_success_rate_nom")|||sum("e-rab_setup_success_rate_den")  as  "e-rab_setup_success_rate" ,
sum("erab_drop_call_rate_nom")|||sum("erab_drop_call_rate_den")  as  "erab_drop_call_rate" ,
sum("handover_in_success_rate_nom")|||sum("handover_in_success_rate_den")  as  "handover_in_success_rate" ,
sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler" ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_nom")|||sum("dl_data_volume_den")  as  "dl_data_volume" ,
sum("ul_data_volume_nom")|||sum("ul_data_volume_den")  as  "ul_data_volume" ,
sum("total_traffic_nom")|||sum("total_traffic_den")  as  "total_traffic" ,
sum("packet_loss_(dl)_nom")|||sum("packet_loss_(dl)_den")  as  "packet_loss_(dl)" ,
sum("packet_loss_(ul)_nom")|||sum("packet_loss_(ul)_den")  as  "packet_loss_(ul)" ,
sum("latency_(only_radio_interface)_nom")|||sum("latency_(only_radio_interface)_den")  as  "latency_(only_radio_interface)" ,
sum("dl_qpsk_nom")  as  "dl_qpsk"  ,
sum("dl_16qam_nom")  as  "dl_16qam"  ,
sum("dl_64qam_nom")  as  "dl_64qam"  ,
sum("dl_256qam_nom")  as  "dl_256qam"  ,
sum("ul_qpsk_nom")  as  "ul_qpsk"  ,
sum("ul_16qam_nom")  as  "ul_16qam"  ,
sum("ul_64qam_nom")  as  "ul_64qam"  ,
sum("ul_256qam_nom")  as  "ul_256qam"  ,
sum("call_setup_success_rate_nom")|||sum("call_setup_success_rate_den")  as  "call_setup_success_rate" ,
sum("e-rab_setup_success_rate_non_gbr_nom")|||sum("e-rab_setup_success_rate_non_gbr_den")  as  "e-rab_setup_success_rate_non_gbr" ,
sum("intrafreq_hosr_nom")|||sum("intrafreq_hosr_den")  as  "intrafreq_hosr" ,
sum("volte_redirection_success_rate_nom")|||sum("volte_redirection_success_rate_den")  as  "volte_redirection_success_rate"
from dt
group by "date_id", rollup("Region")
;


select count(*) from dnb.hourly_stats.dc_e_erbs_eutrancellfdd_raw;









