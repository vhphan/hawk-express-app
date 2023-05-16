create function daily_stats.division(a anyelement, b anyelement) returns double precision
    immutable
    language plpgsql
as
$$
begin
    if b = 0 then
        return null;
    end if;
    return round(cast(a as numeric) / cast(b as numeric), 6) ::double precision;
end;
$$;

create function daily_stats.division(a anyelement, b numeric) returns double precision
    immutable
    language plpgsql
as
$$
begin
    if b = 0 then
        return null;
    end if;
    return round(cast(a as numeric) / cast(b as numeric), 6) ::double precision;
end;
$$;

-- create defined operator to use the user defined function above
create operator ||| (procedure = daily_stats.division, leftarg = anyelement, rightarg = anyelement);

-- create defined operator to use the user defined function above
create operator ||| (procedure = daily_stats.division, leftarg = anyelement, rightarg = numeric);



drop materialized view daily_stats.kpi_nr_nrcelldu;

drop materialized view daily_stats.kpi_nr_nrcelldu;
create materialized view daily_stats.kpi_nr_nrcelldu as
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
-- sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" ,
sum("resource_block_utilizing_rate_dl_nom")|||sum("resource_block_utilizing_rate_dl_den")  as  "resource_block_utilizing_rate_dl" ,
sum("resource_block_utilizing_rate_ul_nom")|||sum("resource_block_utilizing_rate_ul_den")  as  "resource_block_utilizing_rate_ul" ,
sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler"
from
dnb.daily_stats.dc_e_nr_nrcelldu_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");

drop materialized view daily_stats.kpi_nr_nrcellcu;
create materialized view daily_stats.kpi_nr_nrcellcu as
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
dnb.daily_stats.dc_e_nr_nrcellcu_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");

-- check indexes on all tables
select * from pg_indexes where schemaname ilike 'hourly%' and tablename ilike 'dc_e%';

set search_path to daily_stats, public;

-- alter table dc_e_nr_events_nrcelldu_flex_day modify column nrcelldu to varchar(100)

ALTER TABLE dc_e_nr_events_nrcelldu_flex_day ALTER COLUMN nrcelldu TYPE varchar(100);
ALTER TABLE dc_e_nr_events_nrcelldu_flex_day ALTER COLUMN nr_name TYPE varchar(100);

-- alter table dc_e_nr_events_nrcelldu_flex_day modify column nrcelldu to varchar(100)

ALTER TABLE dc_e_nr_events_nrcellcu_flex_day ALTER COLUMN nrcellcu TYPE varchar(100);
ALTER TABLE dc_e_nr_events_nrcellcu_flex_day ALTER COLUMN nr_name TYPE varchar(100);

create index on daily_stats.dc_e_nr_events_nrcellcu_flex_day (nrcellcu);
create index on daily_stats.dc_e_nr_events_nrcelldu_flex_day (nrcelldu);

alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day alter column dl_user_throughput_nom type double precision;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day alter column ul_user_throughput_nom type double precision;

CREATE OR REPLACE FUNCTION to_double1(text)
RETURNS double precision AS $$
  SELECT CASE WHEN upper($1) = 'NULL' THEN NULL
         ELSE $1::double precision END
$$ LANGUAGE sql IMMUTABLE STRICT;

select to_double1(dl_user_throughput_nom) from daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 3 ;

update daily_stats.dc_e_nr_events_nrcelldu_flex_day set dl_user_throughput_nom = null
where dl_user_throughput_nom = 'NULL';

update daily_stats.dc_e_nr_events_nrcelldu_flex_day set ul_user_throughput_nom = null
where ul_user_throughput_nom = 'NULL';

alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day alter column dl_user_throughput_nom type double precision;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day alter column ul_user_throughput_nom type double precision;

alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day add column dl_user_throughput_nom2 double precision;


alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day add column ul_user_throughput_nom2 double precision;


update daily_stats.dc_e_nr_events_nrcelldu_flex_day set dl_user_throughput_nom2 = to_double1(dl_user_throughput_nom);




update daily_stats.dc_e_nr_events_nrcelldu_flex_day set ul_user_throughput_nom2 = to_double1(ul_user_throughput_nom);


select dl_user_throughput_nom, ul_user_throughput_nom from daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 10 ;

-- drop column dl_user_throughput_nom and rename dl_user_throughput_nom2 to dl_user_throughput_nom;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day drop column dl_user_throughput_nom;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day rename column dl_user_throughput_nom2 to dl_user_throughput_nom;



-- drop column dl_user_throughput_nom and rename dl_user_throughput_nom2 to dl_user_throughput_nom;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day drop column ul_user_throughput_nom;
alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day rename column ul_user_throughput_nom2 to ul_user_throughput_nom;



select * from daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 3 ;

SELECT site_id, on_board_date::date, time::date
            FROM dnb.daily_stats.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time

select count(*) from dnb.daily_stats.cell_mapping;

drop materialized view daily_stats.kpi_nr_events_nrcelldu_flex;
create materialized view daily_stats.kpi_nr_events_nrcelldu_flex as
select
date_id,
"Region" as region,
sum("ul_traffic_volume_nom")|||sum("ul_traffic_volume_den")  as  "ul_traffic_volume" ,
sum("dl_qpsk_nom")  as  "dl_qpsk"  ,
sum("dl_16qam_nom")  as  "dl_16qam"  ,
sum("dl_64qam_nom")  as  "dl_64qam"  ,
sum("dl_256qam_nom")  as  "dl_256qam"  ,
sum("ul_qpsk_nom")  as  "ul_qpsk"  ,
sum("ul_16qam_nom")  as  "ul_16qam"  ,
sum("ul_64qam_nom")  as  "ul_64qam"  ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" 
from
dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day as dt
LEFT JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");

drop materialized view daily_stats.kpi_nr_events_nrcellcu_flex;
create materialized view daily_stats.kpi_nr_events_nrcellcu_flex as
select
date_id,
"Region" as region,
sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g"
from
dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day as dt
LEFT JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");

select * from pg_indexes where tablename ilike 'dc%';

alter table daily_stats.dc_e_nr_events_nrcellcu_flex_day alter column flex_filtername type varchar(100);

alter table daily_stats.dc_e_nr_events_nrcelldu_flex_day alter column flex_filtername type varchar(100);

select column_name from information_schema.columns where table_name ilike 'dc%' and column_name ilike '%_nom' and table_name ilike '%_nr_%' order by column_name;




/*markdown

# CELL LEVEL


*/

select nrcelldu from dnb.daily_stats.dc_e_nr_nrcelldu_day order by random() limit 10;

/*markdown
## dc_e_nr_nrcelldu_v_day
*/


alter table daily_stats.dc_e_nr_nrcelldu_v_day alter column nrcelldu type varchar(100);
alter table daily_stats.dc_e_nr_nrcelldu_v_day alter column nr_name type varchar(100);




create index dc_e_nr_nrcelldu_v_day_idx on daily_stats.dc_e_nr_nrcelldu_v_day (nrcelldu);

drop materialized view daily_stats.kpi_nr_nrcelldu_v;
create materialized view daily_stats.kpi_nr_nrcelldu_v as
select
date_id,
"Region" as region,
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
    WHERE "Region" is not null
    GROUP BY date_id, rollup("Region");


alter table daily_stats.dc_e_vpp_rpuserplanelink_v_day alter column ne_name type varchar(100);

create index on daily_stats.dc_e_vpp_rpuserplanelink_v_day (ne_name);

select * from daily_stats.dc_e_vpp_rpuserplanelink_v_day limit 3;

select * from daily_stats.cell_mapping limit 3;


drop materialized view daily_stats.kpi_vpp_rpuserplanelink_v;
create materialized view daily_stats.kpi_vpp_rpuserplanelink_v as
select
date_id,
"Region" as region,
sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul" 
from
dnb.daily_stats.dc_e_vpp_rpuserplanelink_v_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Sitename" = dt."ne_name"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id::date, rollup("Region");



select count( distinct date_id ) from daily_stats.dc_e_vpp_rpuserplanelink_v_day;

select count(distinct date_id) from daily_stats.dc_e_erbsg2_mpprocessingresource_v_day;


drop materialized view daily_stats.kpi_erbsg2_mpprocessingresource_v;



drop materialized view daily_stats.kpi_erbsg2_mpprocessingresource_v;
create materialized view daily_stats.kpi_erbsg2_mpprocessingresource_v as
select
date_id,
"Region" as region,
sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
from
dnb.daily_stats.dc_e_erbsg2_mpprocessingresource_v_day as dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Sitename" = dt."erbs"
    INNER JOIN (SELECT site_id, on_board_date::date, time::date
            FROM dnb.rfdb.df_dpm,
                generate_series(on_board_date::date, now(), '1 day') as time) as obs
                            on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE "Region" is not null
    GROUP BY date_id::date, rollup("Region");


alter table daily_stats.dc_e_erbsg2_mpprocessingresource_v_day alter column erbs type varchar(100);

create index on daily_stats.dc_e_erbsg2_mpprocessingresource_v_day (erbs);

alter table daily_stats.dc_e_erbsg2_mpprocessingresource_v_day rename column gnobeb_cpu_load_nom to gnodeb_cpu_load_nom;

alter table daily_stats.dc_e_erbsg2_mpprocessingresource_v_day rename column gnobeb_cpu_load_den to gnodeb_cpu_load_den;





create unique index on daily_stats.kpi_nr_nrcellcu (date_id, region);
create unique index on daily_stats.kpi_nr_nrcelldu (date_id, region);
create unique index on daily_stats.kpi_nr_nrcelldu_v (date_id, region);
create unique index on daily_stats.kpi_erbsg2_mpprocessingresource_v (date_id, region);
create unique index on daily_stats.kpi_vpp_rpuserplanelink_v (date_id, region);


REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcelldu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcellcu;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcelldu_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_erbsg2_mpprocessingresource_v;
REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_vpp_rpuserplanelink_v;

select distinct region from daily_stats.kpi_nr_nrcelldu_v;



