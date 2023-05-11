
select distinct(date_id) from hourly_stats.dc_e_nr_nrcelldu_raw order by date_id;



drop materialized view if exists hourly_stats.kpi_nr_nrcelldu;



create materialized view hourly_stats.kpi_nr_nrcelldu as
with dt as (select * from dnb.hourly_stats.dc_e_nr_nrcelldu_raw where 
date_id > now() - interval '14 days')
select
date_id,
"Region" as region,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_gb_nom")|||sum("dl_data_volume_gb_den")  as  "dl_data_volume_gb" ,
sum("ul_data_volume_gb_nom")|||sum("ul_data_volume_gb_den")  as  "ul_data_volume_gb" ,
sum("total_traffic_gb_nom")|||sum("total_traffic_gb_den")  as  "total_traffic_gb" ,
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
LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 hour') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null 
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id;


select * FROM hourly_stats.kpi_nr_nrcellcu order by random() limit 3;

drop materialized view hourly_stats.kpi_nr_nrcellcu;

create materialized view hourly_stats.kpi_nr_nrcellcu as
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
dnb.hourly_stats.dc_e_nr_nrcellcu_raw as dt
LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    INNER JOIN (SELECT site_id, on_board_date::date, time
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 hour') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region")
    ORDER BY region, date_id
    ;



create materialized view hourly_stats.kpi_nr_nrcelldu_v as
select
date_id,
"Region" as region,
sum("latency_only_radio_interface_nom")|||sum("latency_only_radio_interface_den")  as  "latency_only_radio_interface" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
from
dnb.hourly_stats.dc_e_nr_nrcelldu_v_raw as dt
LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 hour') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region")
    order by region, date_id
    ;

create materialized view hourly_stats.kpi_vpp_rpuserplanelink_v as
select
date_id,
"Region" as region,
sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul"
from
dnb.hourly_stats.dc_e_vpp_rpuserplanelink_v_raw as dt
LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Sitename" = dt."ne_name"
    INNER JOIN (SELECT site_id, on_board_date::date, time
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 hour') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region")
    order by region, date_id

    ;















create index on dnb.hourly_stats.dc_e_nr_nrcelldu_raw (nrcelldu);

create index on dnb.hourly_stats.dc_e_nr_nrcellcu_raw (nrcellcu);

select * from pg_indexes where tablename ilike 'dc%' and schemaname='hourly_stats'
order by tablename;



select date_id from hourly_stats.dc_e_nr_nrcellcu_raw group by date_id order by random() limit 15;

drop if exists materialized view hourly_stats.kpi_erbsg2_mpprocessingresource_v;

create materialized view hourly_stats.kpi_erbsg2_mpprocessingresource_v as
select
date_id,
"Region" as region,
sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
from
dnb.hourly_stats.dc_e_erbsg2_mpprocessingresource_v_raw as dt
LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Sitename" = dt."erbs"
    INNER JOIN (SELECT site_id, on_board_date::date, time
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 hour') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");

select * from pg_indexes where tablename ilike 'dc_e%' and schemaname='hourly_stats' and indexdef not like '%UNIQUE%';

create index on dnb.hourly_stats.dc_e_erbsg2_mpprocessingresource_v_raw (erbs);

create index on dnb.hourly_stats.dc_e_nr_nrcellcu_raw (nrcellcu);



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



-- check any running process
select * from pg_stat_activity where state = 'active';

-- kill all running process
select pg_terminate_backend(pid) from pg_stat_activity where state = 'active' and pid=4123975;

create index on hourly_stats.dc_e_nr_nrcelldu_raw (date_id);

SELECT t1.site_id, 
t1.on_board_date::timestamp, 

            (select * from dnb.hourly_stats.d_hour as time
                where d_hour.date_actual >= t1.on_board_date::date) as time
            FROM dnb.daily_stats.df_dpm as t1;


alter table hourly_stats.d_hour rename column date_id to datetime_actual;

-- create query to generate each line that corresponds to each hour for each site. the Hour has be greater than the on_board_date.
-- use join on the d_hour table that has the date and hour for each day. Do not use generate_series

create materialized view hourly_stats. as
select t1.site_id,
t1.on_board_date::timestamp,
t2.datetime_actual as time
from dnb.daily_stats.df_dpm as t1
left join dnb.hourly_stats.d_hour as t2 on t2.date_actual >= t1.on_board_date::date
order by t1.site_id, t2.date_actual, t2.datetime_actual;






Select on_board_date from daily_stats.df_dpm order by random() limit 10;