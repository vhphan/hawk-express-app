-- dc_e_erbs_eutrancellfdd_day
drop materialized view if exists dnb.daily_stats.kpi_erbs_eutrancellfdd;
create materialized view dnb.daily_stats.kpi_erbs_eutrancellfdd as
with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select 
date_id,
"Region" as region,
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
sum("dl_qpsk_nom") ||| sum("dl_modulation_den") as  "dl_qpsk"  ,
sum("dl_16qam_nom") ||| sum("dl_modulation_den") as  "dl_16qam"  ,
sum("dl_64qam_nom") ||| sum("dl_modulation_den") as  "dl_64qam"  ,
sum("dl_256qam_nom") ||| sum("dl_modulation_den") as  "dl_256qam"  ,
sum("ul_qpsk_nom") ||| sum("ul_modulation_den") as  "ul_qpsk"  ,
sum("ul_16qam_nom") ||| sum("ul_modulation_den") as  "ul_16qam"  ,
sum("ul_64qam_nom") ||| sum("ul_modulation_den") as  "ul_64qam"  ,
sum("ul_256qam_nom") ||| sum("ul_modulation_den") as  "ul_256qam"  ,
sum("e-rab_setup_success_rate_non_gbr_nom")|||sum("e-rab_setup_success_rate_non_gbr_den")  as  "e-rab_setup_success_rate_non_gbr" ,
sum("intrafreq_hosr_nom")|||sum("intrafreq_hosr_den")  as  "intrafreq_hosr" ,
sum("volte_redirection_success_rate_nom")|||sum("volte_redirection_success_rate_den")  as  "volte_redirection_success_rate"
from dt
group by "date_id", rollup("Region")
;









drop materialized view if exists dnb.daily_stats.kpi_erbs_eutrancellrelation;
create materialized view dnb.daily_stats.kpi_erbs_eutrancellrelation as
with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellrelation_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select
date_id,
"Region" as region,
sum("interfreq_hosr_nom")|||sum("interfreq_hosr_den")  as  "interfreq_hosr"  ,
sum("ifo_success_rate_nom")|||sum("ifo_success_rate_den")  as  "ifo_success_rate"
from dt
group by "date_id", rollup("Region")
;





alter table daily_stats.dc_e_erbs_eutrancellrelation_day rename column ifo_success_rate_denom to ifo_success_rate_den;

drop materialized view if exists dnb.daily_stats.kpi_erbs_eutrancellfdd_v;
create materialized view dnb.daily_stats.kpi_erbs_eutrancellfdd_v as
with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_v_day as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = t1."eutrancellfdd"
        INNER JOIN dnb.rfdb.df_dpm
        on cm."SITEID" = df_dpm.site_id
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
)
select date_id, "Region" as region,
sum("resource_block_utilizing_rate(dl)_nom")|||sum("resource_block_utilizing_rate(dl)_den")  as  "resource_block_utilizing_rate(dl)" ,
sum("resource_block_utilizing_rate(ul)_nom")|||sum("resource_block_utilizing_rate(ul)_den")  as  "resource_block_utilizing_rate(ul)" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"from dt
group by "date_id", rollup("Region")
;







create unique index on daily_stats.kpi_erbs_eutrancellfdd(date_id, region);
create unique index on daily_stats.kpi_erbs_eutrancellrelation(date_id, region);
create unique index on daily_stats.kpi_erbs_eutrancellfdd_v(date_id, region);

refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellfdd;
refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellrelation;
refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellfdd_v;







            select distinct nrcelldu, "Region" as region from daily_stats.dc_e_nr_nrcelldu_day 
            inner join dnb.rfdb.cell_mapping as cm on cm."Cellname" = dc_e_nr_nrcelldu_day.nrcelldu
            where "Region" ilike 'CENTRAl' AND nrcelldu ilike 'DBKLG0493_N3' || '%'
            ORDER BY nrcelldu;



order by nrcelldu limit 10;

select eutrancellfdd from daily_stats.dc_e_erbs_eutrancellfdd_day order by random() limit 10;

with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_day as t1
        where eutrancellfdd = 'DWKUL0414_L7_0030'
)
select 
date_id,
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
from dt group by "date_id"
;

with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellrelation_day as t1
        where eutrancellfdd = 'DWKUL0414_L7_0030'
)
select
date_id,
sum("interfreq_hosr_nom")|||sum("interfreq_hosr_den")  as  "interfreq_hosr"  ,
sum("ifo_success_rate_nom")|||sum("ifo_success_rate_den")  as  "ifo_success_rate"
from dt
group by "date_id"
;

with dt as (
    select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_v_day as t1
        where eutrancellfdd = 'DWKUL0414_L7_0030'
)
select date_id,
sum("resource_block_utilizing_rate(dl)_nom")|||sum("resource_block_utilizing_rate(dl)_den")  as  "resource_block_utilizing_rate(dl)" ,
sum("resource_block_utilizing_rate(ul)_nom")|||sum("resource_block_utilizing_rate(ul)_den")  as  "resource_block_utilizing_rate(ul)" ,
sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"from dt
group by "date_id"
;














create materialized view dnb.daily_stats.cells_list as
select nrcelldu as cell_id, "Region" as region, 'nr' as tech from daily_stats.dc_e_nr_nrcelldu_day
left join dnb.rfdb.cell_mapping as cm on cm."Cellname" = dc_e_nr_nrcelldu_day.nrcelldu
group by nrcelldu, "Region"

union

select eutrancellfdd as cell_id, "Region" as region, 'lte' as tech from daily_stats.dc_e_erbs_eutrancellfdd_day
left join dnb.rfdb.cell_mapping as cm on cm."Cellname" = dc_e_erbs_eutrancellfdd_day.eutrancellfdd
group by eutrancellfdd, "Region"

order by tech, cell_id;
;



create unique index on daily_stats.cells_list(cell_id, region, tech);


refresh materialized view concurrently dnb.daily_stats.cells_list;



























select dl_traffic_volume_den from daily_stats.dc_e_nr_events_nrcelldu_flex_day order by random() limit 10;

select * from information_schema.columns where table_name = 'dc_e_nr_events_nrcelldu_flex_day' order by ordinal_position;





