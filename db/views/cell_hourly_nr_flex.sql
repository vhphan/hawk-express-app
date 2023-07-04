SELECT string_agg( ''''||"nrcelldu"||'''' , ',') FROM (
    
    select "nrcelldu" from
    
        (select distinct "nrcelldu" from dnb.hourly_stats.dc_e_nr_events_nrcelldu_flex_raw) as t0
    
    order by random() limit 30
    
    ) as t;

    -- 'DJJBR0296_N3_0310','DJJBR0410_N3_0330','DWKUL0905_N3_0330','DQMIR0699_N3_0320','DBPET0793_N3_0330',
    -- 'DWKUL0529_N3_0320','DJJBR0579_N3_0310','DAMJG0597_N3_0320','DKKSR0057_N3_0310','DJLED1321_N3_0330',
    -- 'DQMIR0729_N3_0310','DAMLM0732_N3_0330','DWPPJ0814_N3_0320','DMJAS0072_N3_0310','DSLHD0477_N3_0330',
    -- 'DCKTN0448_N3_0310','DBPET0780_N3_0320','DQMIR0657_N3_0310','DBPET1083_N3_0320','DWKUL0440_N3_0320',
    -- 'DJKTT0859_N3_0330','DWKUL0600_N3_0310','DCKTN0278_N3_0330','DWKUL0160_N3_0320','DJJBR0434_N3_0330',
    -- 'DWKUL0336_N3_0330','DBULG1673_N3_0320','DNSBN0349_N3_0320','DBPET1079_N3_0320','DWKUL0099_N3_0320'
    

SELECT string_agg( ''''||"nrcellcu"||'''' , ',') FROM (
    
    select "nrcellcu" from
    
        (select distinct "nrcellcu" from hourly_stats.dc_e_nr_events_nrcellcu_flex_raw) as t0
    
    order by random() limit 30
    
    ) as t;

-- 'DBULG1597_N3_0320','DJSGT1181_N3_0330','DQKCH0509_N3_0330','DWPPJ0864_N3_0330','DBKLG0440_N3_0330','DWKUL0208_N3_0310',
-- 'DJLED1311_N3_0320','DPSPT0117_N3_0320','DQMIR0772_N3_0330','DJMER0957_N3_0330','DPSPU0309_N3_0310','DQKCH0387_N3_0310',
-- 'DJJBR0330_N3_0310','DBGBK0058_N3_0330','DTDUN0043_N3_0310','DQSIB0987_N3_0310','DBKLG0458_N3_0310','DKKSR0102_N3_0320',
-- 'DBGBK0016_N3_0310','DPSPT0224_N3_0330','DWKUL0483_N3_0330','DBPET0972_N3_0330','DWKUL0239_N3_0420','DBULG1745_N3_0320',
-- 'DNPDS0147_N3_0330','DSPNP0582_N3_0310','DBULG1618_N3_0310','DBGBK0118_N3_0330','DCROM0704_N3_0320','DQBTU0093_N3_0320'

-- hourly_stats.kpi_nrcellcu_flex 

with dt as (
    select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_flex_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcellcu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcellcu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'    
)
SELECT
date_id,
mobile_operator,
100 * sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
100 * sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
100 * sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
100 * sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g",
100 * sum("endc_sr_nom")|||sum("endc_sr_den")  as  "endc_sr" ,
100 * sum("erab_drop_nom")|||sum("erab_drop_den")  as  "erab_drop" ,
sum(max_rrc_connected_user_endc) as max_rrc_connected_user_endc,
sum(eps_fallback_attempt) as eps_fallback_attempt 
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcellcu"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DBULG1597_N3_0320','DJSGT1181_N3_0330','DQKCH0509_N3_0330','DWPPJ0864_N3_0330','DBKLG0440_N3_0330','DWKUL0208_N3_0310',
        'DJLED1311_N3_0320','DPSPT0117_N3_0320','DQMIR0772_N3_0330','DJMER0957_N3_0330','DPSPU0309_N3_0310','DQKCH0387_N3_0310',
        'DJJBR0330_N3_0310','DBGBK0058_N3_0330','DTDUN0043_N3_0310','DQSIB0987_N3_0310','DBKLG0458_N3_0310','DKKSR0102_N3_0320',
        'DBGBK0016_N3_0310','DPSPT0224_N3_0330','DWKUL0483_N3_0330','DBPET0972_N3_0330','DWKUL0239_N3_0420','DBULG1745_N3_0320',
        'DNPDS0147_N3_0330','DSPNP0582_N3_0310','DBULG1618_N3_0310','DBGBK0118_N3_0330','DCROM0704_N3_0320','DQBTU0093_N3_0320')
    GROUP BY date_id, mobile_operator;


SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_raw';

-- hourly_stats.kpi_nrcelldu_flex 

with dt as (
    select * from dnb.hourly_stats.dc_e_nr_events_nrcelldu_flex_raw as t1
        INNER JOIN dnb.rfdb.cell_mapping as cm 
            on cm."Cellname" = t1."nrcelldu"
        INNER JOIN dnb.rfdb.df_dpm
            on cm."SITEID" = df_dpm.site_id
        INNER JOIN dnb.rfdb.flex_filters as ff 
            on ff.flex_filtername_nrcelldu = t1.flex_filtername
    WHERE "Region" is not null
    AND t1."date_id" >= df_dpm.on_board_date::timestamp
    AND t1."date_id" > now() - interval '14 days'
)
SELECT
date_id,
mobile_operator,
sum("ul_traffic_volume_nom")|||(1024*1024*1024)  as  "ul_traffic_volume" ,
sum("dl_traffic_volume_nom")|||(1024*1024*1024)  as  "dl_traffic_volume" ,
100 * sum("dl_qpsk_nom")|||sum("dl_modulation_den")  as  "dl_qpsk"  ,
100 * sum("dl_16qam_nom")|||sum("dl_modulation_den")  as  "dl_16qam"  ,
100 * sum("dl_64qam_nom")|||sum("dl_modulation_den")  as  "dl_64qam"  ,
100 * sum("dl_256qam_nom")|||sum("dl_modulation_den")  as  "dl_256qam"  ,
100 * sum("ul_qpsk_nom")|||sum("ul_modulation_den")  as  "ul_qpsk"  ,
100 * sum("ul_16qam_nom")|||sum("ul_modulation_den")  as  "ul_16qam"  ,
100 * sum("ul_64qam_nom")|||sum("ul_modulation_den")  as  "ul_64qam"  ,
sum("dl_user_throughput_nom")|||sum("dl_user_throughput_den")  as  "dl_user_throughput" ,
sum("ul_user_throughput_nom")|||sum("ul_user_throughput_den")  as  "ul_user_throughput" 
from dt
INNER JOIN dnb.rfdb.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
    WHERE cm."Cellname" IN (
        -- list of cells from above
        'DJJBR0296_N3_0310','DJJBR0410_N3_0330','DWKUL0905_N3_0330','DQMIR0699_N3_0320','DBPET0793_N3_0330',
        'DWKUL0529_N3_0320','DJJBR0579_N3_0310','DAMJG0597_N3_0320','DKKSR0057_N3_0310','DJLED1321_N3_0330',
        'DQMIR0729_N3_0310','DAMLM0732_N3_0330','DWPPJ0814_N3_0320','DMJAS0072_N3_0310','DSLHD0477_N3_0330',
        'DCKTN0448_N3_0310','DBPET0780_N3_0320','DQMIR0657_N3_0310','DBPET1083_N3_0320','DWKUL0440_N3_0320',
        'DJKTT0859_N3_0330','DWKUL0600_N3_0310','DCKTN0278_N3_0330','DWKUL0160_N3_0320','DJJBR0434_N3_0330',
        'DWKUL0336_N3_0330','DBULG1673_N3_0320','DNSBN0349_N3_0320','DBPET1079_N3_0320','DWKUL0099_N3_0320')
    GROUP BY date_id,mobile_operator;

/*markdown

*/

select * from pg_indexes where tablename = 'kpi_nrcelldu_flex';



