select * from  dnb.hourly_stats.dc_e_nr_nrcellcu_raw
order by random() limit 5;

select * from  dnb.hourly_stats.dc_e_nr_nrcelldu_raw
order by random() limit 5;

SELECT string_agg( ''''||"nrcelldu"||'''' , ',') FROM (
    
    select "nrcelldu" from
    
        (select distinct "nrcelldu" from hourly_stats.dc_e_nr_nrcelldu_raw) as t0
    
    order by random() limit 30
    
    ) as t;
    

-- 'DBPET1912_N3_0310','DJJBR0405_N3_0320','DWKUL0213_N3_0310','DJJBR0264_N3_0330','DKKMD0269_N3_0320',
-- 'DJJBR0446_N3_0320','DCKTN0214_N3_0310','DDMCH0272_N3_0320','DJPON1136_N3_0320','DSSDK0676_N3_0320',
-- 'DALRM0480_N3_0320','DBPET0873_N3_0330','DJJBR0404_N3_0320','DPSPT0201_N3_0330','DJJBR0143_N3_0320',
-- 'DAKIN0301_N3_0330','DAKIN0240_N3_0320','DBPET1101_N3_0310','DCTMH0785_N3_0330','DNSBN0236_N3_0310',
-- 'DPSPS0086_N3_0330','DQMIR0694_N3_0320','DJJBR0331_N3_0310','DBPET1108_N3_0320','DBULG1655_N3_0330',
-- 'DRPER0010_N3_0310','DKKLM0364_N3_0310','DNSBN0227_N3_0330','DQMIR0705_N3_0320','DSBKI0264_N3_0330'

SELECT string_agg( ''''||"nrcellcu"||'''' , ',') FROM (
    
    select "nrcellcu" from
    
        (select distinct "nrcellcu" from hourly_stats.dc_e_nr_nrcellcu_raw) as t0
    
    order by random() limit 30
    
    ) as t;
    
-- 'DPBDY0034_N3_0330','DBKLG0344_N3_0310','DQMIR0685_N3_0310','DBSEP1341_N3_0330',
-- 'DTKNR0406_N3_0310','DAMJG0590_N3_0320','DPSPU0300_N3_0330','DJBPH0093_N3_0330',
-- 'DJJBR0565_N3_0310','DBKSG0659_N3_0320','DQMIR0764_N3_0330','DNSBN0339_N3_0330',
-- 'DWKUL0472_N3_0330','DWKUL0112_N3_0310','DBPET0990_N3_0320','DALRM0504_N3_0320',
-- 'DBGBK0136_N3_0330','DWKUL0329_N3_0310','DCBNT0027_N3_0310','DSLHD0463_N3_0310',
-- 'DJJBR0314_N3_0310','DQMIR0696_N3_0310','DBSEP1840_N3_0330','DJJBR0317_N3_0310',
-- 'DJJBR0685_N3_0320','DBKSG0658_N3_0310','DQBTU0080_N3_0330','DBULG1589_N3_0330',
-- 'DABAG0675_N3_0310','DWKUL0193_N3_0330'

-- Use SQL TO shift all date_id + 14 days

-- UPDATE dnb.hourly_stats.dc_e_erbsg2_mpprocessingresource_v_raw
-- SET date_id = date_id + INTERVAL '2 weeks'
-- WHERE TRUE;

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
dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DJBPH0042_N3_0320','DBGBK0035_N3_0310','DCROM0705_N3_0330','DBULG1652_N3_0320','DKBDR0045_N3_0310',
        'DBPET0796_N3_0310','DWPPJ0869_N3_0310','DWKUL0563_N3_0330','DAKIN0231_N3_0310','DBULG1613_N3_0330',
        'DWKUL0164_N3_0330','DBULG1496_N3_0320','DJBPH0073_N3_0310','DKPDG0474_N3_0330','DBSEP1393_N3_0320',
        'DKKLM0358_N3_0320','DAKIN0169_N3_0320','DAMJG0547_N3_0310','DPSPT0189_N3_0330','DWKUL0676_N3_0310',
        'DBPET1184_N3_0310','DSBKI0358_N3_0320','DQKCH0511_N3_0310','DNREM0185_N3_0330','DTKTG0288_N3_0330',
        'DTSET0354_N3_0310','DBPET0909_N3_0320','DPSPS0098_N3_0310','DQKCH0404_N3_0330','DSPTT1017_N3_0330')
    GROUP BY date_id;





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
100 * sum("endc_sr_nom")|||sum("endc_sr_den")  as  "endc_sr" ,
100 * sum("erab_drop_call_rate_sgnb_nom")|||sum("erab_drop_call_rate_sgnb_den")  as  "erab_drop_call_rate_sgnb" ,
100 * sum("intra-sgnb_pscell_change_success_rate_nom")|||sum("intra-sgnb_pscell_change_success_rate_den")  as  "intra-sgnb_pscell_change_success_rate" ,
100 * sum("inter-sgnb_pscell_change_success_rate_nom")|||sum("inter-sgnb_pscell_change_success_rate_den")  as  "inter-sgnb_pscell_change_success_rate" ,
100 * sum("rrc_setup_success_rate_signaling_nom")|||sum("rrc_setup_success_rate_signaling_den")  as  "rrc_setup_success_rate_signaling" ,
100 * sum("endc_ca_configuration_sr_nom")|||sum("endc_ca_configuration_sr_den")  as  "endc_ca_configuration_sr" ,
100 * sum("endc_ca_deconfiguration_sr_nom")|||sum("endc_ca_deconfiguration_sr_den")  as  "endc_ca_deconfiguration_sr",
100 * sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" 
from
dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DPBDY0034_N3_0330','DBKLG0344_N3_0310','DQMIR0685_N3_0310','DBSEP1341_N3_0330',
        'DTKNR0406_N3_0310','DAMJG0590_N3_0320','DPSPU0300_N3_0330','DJBPH0093_N3_0330',
        'DJJBR0565_N3_0310','DBKSG0659_N3_0320','DQMIR0764_N3_0330','DNSBN0339_N3_0330',
        'DWKUL0472_N3_0330','DWKUL0112_N3_0310','DBPET0990_N3_0320','DALRM0504_N3_0320',
        'DBGBK0136_N3_0330','DWKUL0329_N3_0310','DCBNT0027_N3_0310','DSLHD0463_N3_0310',
        'DJJBR0314_N3_0310','DQMIR0696_N3_0310','DBSEP1840_N3_0330','DJJBR0317_N3_0310',
        'DJJBR0685_N3_0320','DBKSG0658_N3_0310','DQBTU0080_N3_0330','DBULG1589_N3_0330',
        'DABAG0675_N3_0310','DWKUL0193_N3_0330')
    GROUP BY date_id;


-- -continue from here

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
sum("latency_only_radio_interface_nom")|||sum("latency_only_radio_interface_den")  as  "latency_only_radio_interface" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DJBPH0042_N3_0320','DBGBK0035_N3_0310','DCROM0705_N3_0330','DBULG1652_N3_0320','DKBDR0045_N3_0310',
        'DBPET0796_N3_0310','DWPPJ0869_N3_0310','DWKUL0563_N3_0330','DAKIN0231_N3_0310','DBULG1613_N3_0330',
        'DWKUL0164_N3_0330','DBULG1496_N3_0320','DJBPH0073_N3_0310','DKPDG0474_N3_0330','DBSEP1393_N3_0320',
        'DKKLM0358_N3_0320','DAKIN0169_N3_0320','DAMJG0547_N3_0310','DPSPT0189_N3_0330','DWKUL0676_N3_0310',
        'DBPET1184_N3_0310','DSBKI0358_N3_0320','DQKCH0511_N3_0310','DNREM0185_N3_0330','DTKTG0288_N3_0330',
        'DTSET0354_N3_0310','DBPET0909_N3_0320','DPSPS0098_N3_0310','DQKCH0404_N3_0330','DSPTT1017_N3_0330')
    GROUP BY date_id;


-- SITE LEVEL KPI
-- hourly_stats.kpi_vpp_rpuserplanelink_v 
-- with dt as (
--     select * from (
--         select *, split_part(ne_name, '_', 1) as site_id from dnb.hourly_stats.dc_e_vpp_rpuserplanelink_v_raw
--         ) as t1
--         INNER JOIN rfdb.site_mapping as sm 
--             on sm."SITEID" = t1."site_id"
--         INNER JOIN dnb.rfdb.df_dpm
--             on sm."SITEID" = df_dpm.site_id
--     WHERE "Region" is not null
--     AND t1."date_id" >= df_dpm.on_board_date::timestamp
--     AND t1."date_id" > now() - interval '14 days'
-- )
-- select
-- date_id,
-- ne_name,
-- 100 * sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
-- 100 * sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul"
-- from dt
--     where "Region" is not null
--     GROUP BY date_id, ne_name
--     ORDER BY date_id;

-- SITE LEVEL KPI
-- drop materialized view if exists hourly_stats.kpi_erbsg2_mpprocessingresource_v;
-- create materialized view hourly_stats.kpi_erbsg2_mpprocessingresource_v as
-- with dt as (
--     select * from (
        
--         select *, split_part(erbs, '_', 1) as site_id from dnb.hourly_stats.dc_e_erbsg2_mpprocessingresource_v_raw
        
--         ) as t1
--         INNER JOIN dnb.rfdb.site_mapping as sm 
--         on sm."SITEID" = t1."site_id"
--         INNER JOIN dnb.rfdb.df_dpm
--         on sm."SITEID" = df_dpm.site_id
--             WHERE "Region" is not null
--     AND t1."date_id" >= df_dpm.on_board_date::timestamp
--     AND t1."date_id" > now() - interval '14 days'
--  ) 
-- select
--     date_id,
--     "Region" as region,
--     sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
--     from
--     dt
--     where "Region" is not null
--     GROUP BY date_id, rollup("Region")
--     ;

select date_id, count(*) from dnb.hourly_stats.dc_e_nr_nrcelldu_raw group by date_id order by date_id desc;

