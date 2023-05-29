drop materialized view if exists hourly_stats.kpi_nr_nrcelldu_2;
create materialized view hourly_stats.kpi_nr_nrcelldu_2 as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_nrcelldu_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.rfdb.df_dpm
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

create materialized view hourly_stats.kpi_nr_nrcellcu_2 as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_nrcellcu_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."nrcellcu"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
select
date_id,
"Region" as region,
sum("endc_sr_nom")|||sum("endc_sr_den")  as  "endc_sr" ,
sum("erab_drop_call_rate_sgnb_nom")|||sum("erab_drop_call_rate_sgnb_den")  as  "erab_drop_call_rate_sgnb" ,
sum("intra-sgnb_pscell_change_success_rate_nom")|||sum("intra-sgnb_pscell_change_success_rate_den")  as  "intra-sgnb_pscell_change_success_rate" ,
sum("inter-sgnb_pscell_change_success_rate_nom")|||sum("inter-sgnb_pscell_change_success_rate_den")  as  "inter-sgnb_pscell_change_success_rate" ,
sum("rrc_setup_success_rate_signaling_nom")|||sum("rrc_setup_success_rate_signaling_den")  as  "rrc_setup_success_rate_signaling" ,
sum("endc_ca_configuration_sr_nom")|||sum("endc_ca_configuration_sr_den")  as  "endc_ca_configuration_sr" ,
sum("endc_ca_deconfiguration_sr_nom")|||sum("endc_ca_deconfiguration_sr_den")  as  "endc_ca_deconfiguration_sr",
sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" 
from
dt
    where "Region" is not null
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id;


create materialized view hourly_stats.kpi_nr_nrcelldu_v_2 as
with dt as (
    select * from dnb.hourly_stats.dc_e_nr_nrcelldu_v_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
select
date_id,
"Region" as region,
sum("latency_only_radio_interface_nom")|||sum("latency_only_radio_interface_den")  as  "latency_only_radio_interface" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
from dt
    where "Region" is not null
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id;

drop materialized view if exists rfdb.site_mapping cascade;
create materialized view rfdb.site_mapping as
select 
"SITEID",
-- "Sitename",
"Region"
from rfdb.cell_mapping
inner join rfdb.df_dpm on rfdb.cell_mapping."SITEID" = rfdb.df_dpm.site_id
GROUP by 
"SITEID",
-- "Sitename",
"Region"
;


create materialized view hourly_stats.kpi_vpp_rpuserplanelink_v_2 as
with dt as (
    select * from (
        select *, split_part(ne_name, '_', 1) as site_id from dnb.hourly_stats.dc_e_vpp_rpuserplanelink_v_raw
        ) as t1
        INNER JOIN rfdb.site_mapping as sm 
            on sm."SITEID" = t1."site_id"
        INNER JOIN dnb.rfdb.df_dpm
            on sm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
select
date_id,
"Region" as region,
sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul"
from dt
    where "Region" is not null
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id;

-- TODO Cells below

drop materialized view if exists hourly_stats.kpi_erbsg2_mpprocessingresource_v_2;
create materialized view hourly_stats.kpi_erbsg2_mpprocessingresource_v_2 as
with dt as (
    select * from (
        
        select *, split_part(erbs, '_', 1) as site_id from dnb.hourly_stats.dc_e_erbsg2_mpprocessingresource_v_raw
        
        ) as t1
        INNER JOIN dnb.rfdb.site_mapping as sm 
        on sm."SITEID" = t1."site_id"
        INNER JOIN dnb.rfdb.df_dpm
        on sm."SITEID" = df_dpm.site_id
            WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
 ) 
select
    date_id,
    "Region" as region,
    sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
    from
    dt
    where "Region" is not null
    GROUP BY date_id, rollup("Region")
    ;


    ALTER MATERIALIZED VIEW dnb.hourly_stats.kpi_nr_nrcelldu_2 rename to kpi_nr_nrcelldu;
    ALTER MATERIALIZED VIEW dnb.hourly_stats.kpi_nr_nrcellcu_2 rename to kpi_nr_nrcellcu;
    ALTER MATERIALIZED VIEW dnb.hourly_stats.kpi_nr_nrcelldu_v_2 rename  to kpi_nr_nrcelldu_v;
    ALTER MATERIALIZED VIEW dnb.hourly_stats.kpi_erbsg2_mpprocessingresource_v_2 rename to kpi_erbsg2_mpprocessingresource_v; ;
    ALTER MATERIALIZED VIEW dnb.hourly_stats.kpi_vpp_rpuserplanelink_v_2 rename to  kpi_vpp_rpuserplanelink_v;        



create unique index on hourly_stats.kpi_nr_nrcellcu (date_id, region);
create unique index on hourly_stats.kpi_nr_nrcelldu (date_id, region);
create unique index on hourly_stats.kpi_nr_nrcelldu_v (date_id, region);
create unique index on hourly_stats.kpi_erbsg2_mpprocessingresource_v (date_id, region);
create unique index on hourly_stats.kpi_vpp_rpuserplanelink_v (date_id, region);


REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcelldu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcellcu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcelldu_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_erbsg2_mpprocessingresource_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_vpp_rpuserplanelink_v;



select date_id, count(*) from dnb.hourly_stats.dc_e_nr_nrcelldu_raw group by date_id order by date_id desc;

