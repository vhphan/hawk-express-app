drop materialized view if exists daily_stats.clusters_kpi_nr_nrcelldu;
create materialized view daily_stats.clusters_kpi_nr_nrcelldu as
select
date_id,
"Cluster_ID" as cluster_id,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_gb_nom")|||power(1024,3)  as  "dl_data_volume_gb" ,
sum("ul_data_volume_gb_nom")|||power(1024,3)  as  "ul_data_volume_gb" ,
sum("total_traffic_gb_nom")|||power(1024,3) as  "total_traffic_gb" ,
100 * sum("dl_qpsk_nom") ||| sum("dl_modulation_den") as  "dl_qpsk"  ,
100 * sum("dl_16qam_nom") ||| sum("dl_modulation_den") as  "dl_16qam"  ,
100 * sum("dl_64qam_nom") ||| sum("dl_modulation_den") as  "dl_64qam"  ,
100 * sum("dl_256qam_nom") ||| sum("dl_modulation_den") as  "dl_256qam"  ,
100 * sum("ul_qpsk_nom") ||| sum("ul_modulation_den")  as  "ul_qpsk"  ,
100 * sum("ul_16qam_nom") ||| sum("ul_modulation_den") as  "ul_16qam"  ,
100 * sum("ul_64qam_nom") ||| sum("ul_modulation_den") as  "ul_64qam"  ,
100 * sum("ul_256qam_nom") ||| sum("ul_modulation_den") as  "ul_256qam"  ,
sum("dl_mac_vol_to_scell_nom")||| power(1024,3) as "dl_mac_vol_to_scell" ,
sum("dl_mac_vol_as_scell_nom")||| power(1024,3) as "dl_mac_vol_as_scell" ,
sum("dl_mac_vol_to_scell_ext_nom")||| power(1024,3) as "dl_mac_vol_to_scell_ext" ,
sum("dl_mac_vol_as_scell_ext_nom")||| power(1024,3) as "dl_mac_vol_as_scell_ext" ,
100 * sum("cell_availability_nom")|||sum("cell_availability_den")  as  "cell_availability" ,
-- sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" ,
100 * sum("resource_block_utilizing_rate_dl_nom")|||sum("resource_block_utilizing_rate_dl_den")  as  "resource_block_utilizing_rate_dl" ,
100 * sum("resource_block_utilizing_rate_ul_nom")|||sum("resource_block_utilizing_rate_ul_den")  as  "resource_block_utilizing_rate_ul" ,
100 * sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler"
from
dnb.daily_stats.dc_e_nr_nrcelldu_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Cluster_ID" is not null
    GROUP BY date_id, "Cluster_ID";

create unique index on daily_stats.clusters_kpi_nr_nrcelldu (date_id, cluster_id);

drop materialized view if exists daily_stats.clusters_kpi_nr_nrcellcu;
create materialized view daily_stats.clusters_kpi_nr_nrcellcu as
select
date_id,
"Cluster_ID" as cluster_id,
100 * sum("endc_sr_nom")|||sum("endc_sr_den")  as  "endc_sr" ,
100 * sum("erab_drop_call_rate_sgnb_nom")|||sum("erab_drop_call_rate_sgnb_den")  as  "erab_drop_call_rate_sgnb" ,
100 * sum("intra-sgnb_pscell_change_success_rate_nom")|||sum("intra-sgnb_pscell_change_success_rate_den")  as  "intra-sgnb_pscell_change_success_rate" ,
100 * sum("inter-sgnb_pscell_change_success_rate_nom")|||sum("inter-sgnb_pscell_change_success_rate_den")  as  "inter-sgnb_pscell_change_success_rate" ,
100 * sum("rrc_setup_success_rate_signaling_nom")|||sum("rrc_setup_success_rate_signaling_den")  as  "rrc_setup_success_rate_signaling" ,
100 * sum("endc_ca_configuration_sr_nom")|||sum("endc_ca_configuration_sr_den")  as  "endc_ca_configuration_sr" ,
100 * sum("endc_ca_deconfiguration_sr_nom")|||sum("endc_ca_deconfiguration_sr_den")  as  "endc_ca_deconfiguration_sr",
100 * sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" 
from
dnb.daily_stats.dc_e_nr_nrcellcu_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Cluster_ID" is not null
    GROUP BY date_id, "Cluster_ID";

create unique index on daily_stats.clusters_kpi_nr_nrcellcu (date_id, cluster_id);



refresh materialized view concurrently daily_stats.clusters_kpi_nr_nrcelldu;
refresh materialized view concurrently daily_stats.clusters_kpi_nr_nrcellcu;



drop materialized view if exists daily_stats.clusters_kpi_nr_nrcelldu_v;

create materialized view daily_stats.clusters_kpi_nr_nrcelldu_v as
select
date_id,
"Cluster_ID" as cluster_id,
sum("latency_only_radio_interface_nom")|||sum("latency_only_radio_interface_den")  as  "latency_only_radio_interface" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
from
dnb.daily_stats.dc_e_nr_nrcelldu_v_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Cluster_ID" is not null
    GROUP BY date_id, "Cluster_ID";



create unique index on daily_stats.clusters_kpi_nr_nrcelldu_v (date_id, cluster_id);

refresh materialized view concurrently daily_stats.clusters_kpi_nr_nrcelldu_v;


drop materialized view if exists daily_stats.clusters_kpi_vpp_rpuserplanelink_v;
create materialized view daily_stats.clusters_kpi_vpp_rpuserplanelink_v as
select
date_id,
"Cluster_ID" as cluster_id,
100 * sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
100 * sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul" 
from
dnb.daily_stats.dc_e_vpp_rpuserplanelink_v_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Sitename" = dt."ne_name"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, "Cluster_ID";

create unique index on daily_stats.clusters_kpi_vpp_rpuserplanelink_v (date_id, cluster_id);

refresh materialized view concurrently daily_stats.clusters_kpi_vpp_rpuserplanelink_v;

drop materialized view if exists daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v;
create materialized view daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v as
select
date_id,
"Cluster_ID" as cluster_id,
sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
from
dnb.daily_stats.dc_e_erbsg2_mpprocessingresource_v_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Sitename" = dt."erbs"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, "Cluster_ID";

create unique index on daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v (date_id, cluster_id);

REFRESH MATERIALIZED VIEW CONCURRENTLY daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v;


REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.clusters_kpi_nr_nrcelldu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.clusters_kpi_nr_nrcellcu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.clusters_kpi_nr_nrcelldu_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.clusters_kpi_vpp_rpuserplanelink_v;

