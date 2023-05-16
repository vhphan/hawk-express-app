const sql = require('../db/PgBackend');
const { logger } = require('../middleware/logger');



const getCellDailyStatsNR = async (cellId, tableName) => {

    if (tableName === 'dc_e_nr_nrcelldu_day') {
        return await sql`
    select
        date_id,
        nrcelldu,
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
    dnb.daily_stats.dc_e_nr_nrcelldu_day as dt
    LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
                INNER JOIN (SELECT site_id, on_board_date::date, time::date
                        FROM dnb.daily_stats.df_dpm,
                            generate_series(on_board_date::date, now(), '1 day') as time) as obs
                                        on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
    WHERE nrcelldu=${cellId}
    GROUP BY date_id, nrcelldu;
        `;
    }

    if (tableName === 'dc_e_nr_nrcellcu_day') {
        return await sql`
        select
            date_id,
            nrcellcu,
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
        LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
                INNER JOIN (SELECT site_id, on_board_date::date, time::date
                        FROM dnb.daily_stats.df_dpm,
                            generate_series(on_board_date::date, now(), '1 day') as time) as obs
                                        on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
        WHERE nrcellcu=${cellId}
        GROUP BY date_id, nrcellcu;
        `;
    }


    if (tableName === 'dc_e_nr_nrcelldu_v_day') {
        return await sql`
        select
            date_id,
            sum("latency_only_radio_interface_nom")|||sum("latency_only_radio_interface_den")  as  "latency_only_radio_interface" ,
            sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
            sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"
            from
            dnb.daily_stats.dc_e_nr_nrcelldu_v_day as dt
            LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Cellname" = dt."nrcelldu"
                INNER JOIN (SELECT site_id, on_board_date::date, time::date
                        FROM dnb.daily_stats.df_dpm,
                            generate_series(on_board_date::date, now(), '1 day') as time) as obs
                                        on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
                WHERE nrcelldu = ${cellId}
                GROUP BY date_id;
        `;
    }

    if (tableName === 'dc_e_erbsg2_mpprocessingresource_v_day') {
        const siteId = cellId.split('_')[0];
        return await sql`
        select
            date_id,
            sum("gnodeb_cpu_load_nom")|||sum("gnodeb_cpu_load_den")  as  "gnodeb_cpu_load"
            from
            dnb.daily_stats.dc_e_erbsg2_mpprocessingresource_v_day as dt
            LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Sitename" = dt."erbs"
                INNER JOIN (SELECT site_id, on_board_date::date, time::date
                        FROM dnb.daily_stats.df_dpm,
                            generate_series(on_board_date::date, now(), '1 day') as time) as obs
                                        on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
                WHERE erbs like ${siteId} || '_%'
                GROUP BY date_id
                order by date_id
                ;

        `;
    }

    if (tableName === 'dc_e_vpp_rpuserplanelink_v_day') {
        const siteId = cellId.split('_')[0];
        return await sql`
        select
            date_id,
            sum("packet_loss_dl_nom")|||sum("packet_loss_dl_den")  as  "packet_loss_dl" ,
            sum("packet_loss_ul_nom")|||sum("packet_loss_ul_den")  as  "packet_loss_ul"
            from
            dnb.daily_stats.dc_e_vpp_rpuserplanelink_v_day as dt
            LEFT JOIN dnb.daily_stats.cell_mapping as cm on cm."Sitename" = dt."ne_name"
                INNER JOIN (SELECT site_id, on_board_date::date, time::date
                        FROM dnb.daily_stats.df_dpm,
                            generate_series(on_board_date::date, now(), '1 day') as time) as obs
                                        on obs.time = dt."date_id" and cm."SITEID" = obs.site_id
                WHERE ne_name like ${siteId} || '_%'
                GROUP BY date_id
                ;
        `;
    }
}

const getCellDailyStatsLTE = async (tableName) => {
    throw new Error('Not implemented');
}

const getRegionDailyStatsLTE = async (tableName) => {

    if (tableName === 'dc_e_erbs_eutrancellfdd_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_erbs_eutrancellfdd as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_erbs_eutrancellfdd_v_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_erbs_eutrancellfdd_v as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_erbs_eutrancellrelation_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_erbs_eutrancellrelation as dt order by date_id
        `;
    }

}

const getRegionDailyStatsNR = async (tableName) => {

    // 'dc_e_nr_nrcelldu_day',
    // 'dc_e_nr_nrcellcu_day',
    // 'dc_e_nr_nrcelldu_v_day',
    // 'dc_e_erbsg2_mpprocessingresource_v_day',
    // 'dc_e_vpp_rpuserplanelink_v_day',

    if (tableName === 'dc_e_nr_nrcelldu_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_nr_nrcelldu as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_nrcellcu_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_nr_nrcellcu as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_nrcelldu_v_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_nr_nrcelldu_v as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_erbsg2_mpprocessingresource_v_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_erbsg2_mpprocessingresource_v as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_vpp_rpuserplanelink_v_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_vpp_rpuserplanelink_v as dt order by date_id
        `;
    }


    throw new Error('Invalid table name for NR region daily stats; ' + tableName);

}

const refreshMaterializedViews = async () => {
    // NR
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcelldu;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcellcu;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_nr_nrcelldu_v;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_erbsg2_mpprocessingresource_v;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.daily_stats.kpi_vpp_rpuserplanelink_v;
    `;

    // FLEX
    await sql`
    refresh materialized view concurrently daily_stats.kpi_eutrancellfdd_flex;
    `;
    await sql`
    refresh materialized view concurrently daily_stats.kpi_nrcellcu_flex;
    `;
    await sql`
    refresh materialized view concurrently daily_stats.kpi_nrcelldu_flex;
    `;

    // LTE
    await sql`
    refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellfdd;
    `;
    await sql`
    refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellrelation;
    `;
    await sql`
    refresh materialized view concurrently dnb.daily_stats.kpi_erbs_eutrancellfdd_v;
    `;


};

const refreshMaterializedViewsHourly = async () => {
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcelldu;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcellcu;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_nr_nrcelldu_v;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_erbsg2_mpprocessingresource_v;
    `;
    await sql`
    REFRESH MATERIALIZED VIEW CONCURRENTLY dnb.hourly_stats.kpi_vpp_rpuserplanelink_v;
    `;
};


const createCronToRefreshMaterializedViews = async () => {

    logger.info('Creating cron to refresh materialized views');
    await refreshMaterializedViews();
    await refreshMaterializedViewsHourly();
    const cron = require('node-cron');

    cron.schedule('30 3 * * *', async () => {
        await refreshMaterializedViews();
    });

    cron.schedule('30 4 * * *', async () => {
        await refreshMaterializedViewsHourly();
    });

    cron.schedule('30 9 * * *', async () => {
        await refreshMaterializedViewsHourly();
    });

    cron.schedule('30 13 * * *', async () => {
        await refreshMaterializedViewsHourly();
    });

    cron.schedule('30 16 * * *', async () => {
        await refreshMaterializedViewsHourly();
    });


}




module.exports = {
    getCellDailyStatsNR,
    getCellDailyStatsLTE,
    getRegionDailyStatsNR,
    getRegionDailyStatsLTE,
    createCronToRefreshMaterializedViews
}