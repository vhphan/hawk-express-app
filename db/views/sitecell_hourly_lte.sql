-- erbs_eutrancellfdd
select * from  dnb.hourly_stats.dc_e_erbs_eutrancellfdd_raw
order by random() limit 5;

    select "eutrancellfdd" from
    (select distinct "eutrancellfdd" from hourly_stats.dc_e_erbs_eutrancellfdd_raw) as t0
     order by random() limit 10


-- get comma separated list of cells randomly

SELECT string_agg( ''''||"eutrancellfdd"||'''' , ',') FROM (
    
    select "eutrancellfdd" from
    
        (select distinct "eutrancellfdd" from hourly_stats.dc_e_erbs_eutrancellfdd_raw) as t0
    
    order by random() limit 30
    
    ) as t;

"string_agg"
-- "'DAKIN0176_L7_0030','DQBTU0170_L7_0020','DBPET0819_L7_0020','DJJBR0375_L7_0020','DWKUL0685_L7_0020','DJSGT1178_L7_0010','DDKBR0133_L7_0010','DCPKN0618_L7_0020','DJJBR0317_L7_0010','DJKLJ1258_L7_0030','DQSIB0987_L7_0130','DBKLG0341_L7_0030','DAKIN0344_L7_0030','DJJBR0373_L7_0010','DWKUL0529_L7_0020','DPSPT0150_L7_0110','DQMIR0706_L7_0030','DDKBR0134_L7_0030','DPTML0336_L7_0020','DSBKI0325_L7_0030','DQMIR0695_L7_0020','DSBKI0338_L7_0020','DKKMD0268_L7_0030','DJBPH0117_L7_0020','DDTMR0397_L7_0020','DBGBK0087_L7_0030','DJLED1311_L7_0010','DBPET0917_L7_0010','DWPPJ0897_L7_0030','DQKCH0269_L7_0030'"


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
100 * sum("call_setup_success_rate_nom")|||sum("call_setup_success_rate_den")  as  "call_setup_success_rate" ,
100 * sum("cell_availability_nom")|||sum("cell_availability_den")  as  "cell_availability" ,
100 * sum("rrc_setup_success_rate_(service)_nom")|||sum("rrc_setup_success_rate_(service)_den")  as  "rrc_setup_success_rate_(service)" ,
100 * sum("rrc_setup_success_rate_(signaling)_nom")|||sum("rrc_setup_success_rate_(signaling)_den")  as  "rrc_setup_success_rate_(signaling)" ,
100 * sum("e-rab_setup_success_rate_nom")|||sum("e-rab_setup_success_rate_den")  as  "e-rab_setup_success_rate" ,
100 * sum("erab_drop_call_rate_nom")|||sum("erab_drop_call_rate_den")  as  "erab_drop_call_rate" ,
100 * sum("handover_in_success_rate_nom")|||sum("handover_in_success_rate_den")  as  "handover_in_success_rate" ,
100 * sum("ul_bler_nom")|||sum("ul_bler_den")  as  "ul_bler" ,
100 * sum("packet_loss_(dl)_nom")|||sum("packet_loss_(dl)_den")  as  "packet_loss_(dl)" ,
100 * sum("packet_loss_(ul)_nom")|||sum("packet_loss_(ul)_den")  as  "packet_loss_(ul)" ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" ,
sum("dl_cell_throughput_nom")|||sum("dl_cell_throughput_den")  as  "dl_cell_throughput" ,
sum("ul_cell_throughput_nom")|||sum("ul_cell_throughput_den")  as  "ul_cell_throughput" ,
sum("dl_data_volume_nom")|||8388608  as  "dl_data_volume" ,
sum("ul_data_volume_nom")|||8388608  as  "ul_data_volume" ,
sum("total_traffic_nom")|||8388608 as  "total_traffic" ,
sum("latency_(only_radio_interface)_nom")|||sum("latency_(only_radio_interface)_den")  as  "latency_(only_radio_interface)" , 
100 * sum("dl_qpsk_nom") ||| sum("dl_modulation_den") as  "dl_qpsk"  , 
100 * sum("dl_16qam_nom") ||| sum("dl_modulation_den") as  "dl_16qam"  , 
100 * sum("dl_64qam_nom") ||| sum("dl_modulation_den") as  "dl_64qam"  , 
100 * sum("dl_256qam_nom") ||| sum("dl_modulation_den") as  "dl_256qam"  , 
100 * sum("ul_qpsk_nom") ||| sum("ul_modulation_den") as  "ul_qpsk"  , 
100 * sum("ul_16qam_nom") ||| sum("ul_modulation_den") as  "ul_16qam"  , 
100 * sum("ul_64qam_nom") ||| sum("ul_modulation_den") as  "ul_64qam"  , 
100 * sum("ul_256qam_nom") ||| sum("ul_modulation_den") as  "ul_256qam"  ,
sum("e-rab_setup_success_rate_non_gbr_nom")|||sum("e-rab_setup_success_rate_non_gbr_den")  as  "e-rab_setup_success_rate_non_gbr" ,
sum("intrafreq_hosr_nom")|||sum("intrafreq_hosr_den")  as  "intrafreq_hosr" ,
sum("volte_redirection_success_rate_nom")|||sum("volte_redirection_success_rate_den")  as  "volte_redirection_success_rate"
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."eutrancellfdd"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DQBTU0170_L7_0020',
        'DBPET0819_L7_0020','DJJBR0375_L7_0020','DWKUL0685_L7_0020',
        'DJSGT1178_L7_0010','DDKBR0133_L7_0010','DCPKN0618_L7_0020','DJJBR0317_L7_0010','DJKLJ1258_L7_0030',
        'DQSIB0987_L7_0130','DBKLG0341_L7_0030','DAKIN0344_L7_0030','DJJBR0373_L7_0010','DWKUL0529_L7_0020',
        'DPSPT0150_L7_0110','DQMIR0706_L7_0030','DDKBR0134_L7_0030','DPTML0336_L7_0020','DSBKI0325_L7_0030',
        'DQMIR0695_L7_0020','DSBKI0338_L7_0020','DKKMD0268_L7_0030','DJBPH0117_L7_0020','DDTMR0397_L7_0020',
        'DBGBK0087_L7_0030','DJLED1311_L7_0010','DBPET0917_L7_0010','DWPPJ0897_L7_0030','DQKCH0269_L7_0030')
    GROUP BY date_id;



select date_id, count(*) from dnb.hourly_stats.dc_e_erbs_eutrancellrelation_raw
group by date_id

select * from dnb.rfdb.cell_mapping;

--erbs_eutrancellrelation

with dt as (
    select * from dnb.hourly_stats.dc_e_erbs_eutrancellrelation_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select
date_id,
sum("interfreq_hosr_nom")|||sum("interfreq_hosr_den")  as  "interfreq_hosr"  ,
sum("ifo_success_rate_nom")|||sum("ifo_success_rate_den")  as  "ifo_success_rate"
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."eutrancellfdd"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DQBTU0170_L7_0020',
        'DBPET0819_L7_0020','DJJBR0375_L7_0020','DWKUL0685_L7_0020',
        'DJSGT1178_L7_0010','DDKBR0133_L7_0010','DCPKN0618_L7_0020','DJJBR0317_L7_0010','DJKLJ1258_L7_0030',
        'DQSIB0987_L7_0130','DBKLG0341_L7_0030','DAKIN0344_L7_0030','DJJBR0373_L7_0010','DWKUL0529_L7_0020',
        'DPSPT0150_L7_0110','DQMIR0706_L7_0030','DDKBR0134_L7_0030','DPTML0336_L7_0020','DSBKI0325_L7_0030',
        'DQMIR0695_L7_0020','DSBKI0338_L7_0020','DKKMD0268_L7_0030','DJBPH0117_L7_0020','DDTMR0397_L7_0020',
        'DBGBK0087_L7_0030','DJLED1311_L7_0010','DBPET0917_L7_0010','DWPPJ0897_L7_0030','DQKCH0269_L7_0030')
    GROUP BY date_id;

create unique index on hourly_stats.clusters_kpi_erbs_eutrancellrelation(date_id, cluster_id);


--erbs_eutrancellfdd_v

with dt as (
    select * from dnb.hourly_stats.dc_e_erbs_eutrancellfdd_v_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select date_id,
sum("resource_block_utilizing_rate(dl)_nom")|||sum("resource_block_utilizing_rate(dl)_den")  as  "resource_block_utilizing_rate(dl)" ,
sum("resource_block_utilizing_rate(ul)_nom")|||sum("resource_block_utilizing_rate(ul)_den")  as  "resource_block_utilizing_rate(ul)" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."eutrancellfdd"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DQBTU0170_L7_0020',
        'DBPET0819_L7_0020','DJJBR0375_L7_0020','DWKUL0685_L7_0020',
        'DJSGT1178_L7_0010','DDKBR0133_L7_0010','DCPKN0618_L7_0020','DJJBR0317_L7_0010','DJKLJ1258_L7_0030',
        'DQSIB0987_L7_0130','DBKLG0341_L7_0030','DAKIN0344_L7_0030','DJJBR0373_L7_0010','DWKUL0529_L7_0020',
        'DPSPT0150_L7_0110','DQMIR0706_L7_0030','DDKBR0134_L7_0030','DPTML0336_L7_0020','DSBKI0325_L7_0030',
        'DQMIR0695_L7_0020','DSBKI0338_L7_0020','DKKMD0268_L7_0030','DJBPH0117_L7_0020','DDTMR0397_L7_0020',
        'DBGBK0087_L7_0030','DJLED1311_L7_0010','DBPET0917_L7_0010','DWPPJ0897_L7_0030','DQKCH0269_L7_0030')
    GROUP BY date_id;
