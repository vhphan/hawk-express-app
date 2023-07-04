UPDATE dnb.hourly_stats.dc_e_erbs_eutrancellfdd_flex_raw
SET date_id = date_id + INTERVAL '2 weeks'
WHERE TRUE;

SELECT string_agg( ''''||"eutrancellfdd"||'''' , ',') FROM (
    
    select "eutrancellfdd" from
    
        (select distinct "eutrancellfdd" from hourly_stats.dc_e_erbs_eutrancellfdd_flex_raw) as t0
    
    order by random() limit 30
    
    ) as t;

-- 'DQKCH0294_L7_0010','DBKLG0534_L7_0020','DBKSG0655_L7_0030','DWKUL0150_L7_0030','DBHSG0242_L7_0020',
-- 'DJJBR0403_L7_0030','DWKUL0638_L7_0010','DWKUL0981_L7_0010','DQKCH0266_L7_0030','DQKCH0353_L7_0030',
-- 'DJJBR0446_L7_0030','DJJBR0394_L7_0020','DBKLG0310_L7_0010','DKPDG0479_L7_0010','DBKSG0667_L7_0010',
-- 'DBULG1754_L7_0010','DQMIR0746_L7_0010','DCBNT0004_L7_0130','DNSBN0353_L7_0010','DJJBR0739_L7_0010',
-- 'DNSBN0206_L7_0020','DTKTG0206_L7_0010','DWKUL0553_L7_0010','DNSBN0229_L7_0020','DMMKT0187_L7_0020',
-- 'DBKSG0680_L7_0010','DBKLG0355_L7_0020','DTKNR0381_L7_0030','DSLHD0469_L7_0020','DJKLJ1267_L7_0030'

-- hourly_stats.kpi_eutrancellfdd_flex 

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
    AND t1."date_id" > now() - interval '14 days'
)
SELECT
date_id,
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
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."eutrancellfdd"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DQKCH0294_L7_0010','DBKLG0534_L7_0020','DBKSG0655_L7_0030','DWKUL0150_L7_0030','DBHSG0242_L7_0020',
        'DJJBR0403_L7_0030','DWKUL0638_L7_0010','DWKUL0981_L7_0010','DQKCH0266_L7_0030','DQKCH0353_L7_0030',
        'DJJBR0446_L7_0030','DJJBR0394_L7_0020','DBKLG0310_L7_0010','DKPDG0479_L7_0010','DBKSG0667_L7_0010',
        'DBULG1754_L7_0010','DQMIR0746_L7_0010','DCBNT0004_L7_0130','DNSBN0353_L7_0010','DJJBR0739_L7_0010',
        'DNSBN0206_L7_0020','DTKTG0206_L7_0010','DWKUL0553_L7_0010','DNSBN0229_L7_0020','DMMKT0187_L7_0020',
        'DBKSG0680_L7_0010','DBKLG0355_L7_0020','DTKNR0381_L7_0030','DSLHD0469_L7_0020','DJKLJ1267_L7_0030')
    GROUP BY date_id,mobile_operator;