create materialized view hourly_stats.kpi_nrcellcu_flex as
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
)
SELECT
date_id,
"Region" as region,
mobile_operator,
sum("intra-sgnb_pscell_change_success_nom")|||sum("intra-sgnb_pscell_change_success_den")  as  "intra-sgnb_pscell_change_success" ,
sum("inter-sgnb_pscell_change_success_nom")|||sum("inter-sgnb_pscell_change_success_den")  as  "inter-sgnb_pscell_change_success" ,
sum("5g_ho_success_rate_dnb_5g_to_dnb_nom")|||sum("5g_ho_success_rate_dnb_5g_to_dnb_den")  as  "5g_ho_success_rate_dnb_5g_to_dnb" ,
sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_nom")|||sum("inter_rat_ho_success_rate_dnb_5g_to_mno_4g_den")  as  "inter_rat_ho_success_rate_dnb_5g_to_mno_4g" 
from dt
group by date_id, mobile_operator, rollup("Region")
order by region, date_id;

SELECT * FROM pg_indexes WHERE tablename = 'dc_e_nr_events_nrcellcu_flex_raw';


create index on dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day (flex_filtername);

-- drop index using index name
drop index if exists dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_day_nrcellcu_idx1;


select * from pg_indexes where tablename = 'dc_e_nr_events_nrcelldu_flex_day';







select date_id, nrcelldu, flex_filtername, count(*) 
from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day 
where flex_filtername is not null
group by date_id, nrcelldu, flex_filtername 
having count(*) > 1 order by random() limit 5;

select date_id, count(*) from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day group by date_id order by date_id;


select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_day where nrcelldu = 'DWKUL0421_N3_0310'
and date_id = '2023-04-26'
and flex_filtername = 'UeGrpId5'
order by nrcelldu, flex_filtername

;



