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
                WHERE ne_name like ${siteId} || '_%'
                GROUP BY date_id
                ;
        `;
    }
}

const getCellDailyStatsLTE = async (cellId, tableName) => {

    if (tableName === 'dc_e_erbs_eutrancellfdd_day') {

        return await sql`
        with dt as (
            select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_day as t1
                where eutrancellfdd = ${cellId}
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
        ; `;

    }

    if (tableName === 'dc_e_erbs_eutrancellrelation_day') {

        return await sql`
        with dt as (
            select * from dnb.daily_stats.dc_e_erbs_eutrancellrelation_day as t1
                where eutrancellfdd = ${cellId}
        )
        select
        date_id,
        sum("interfreq_hosr_nom")|||sum("interfreq_hosr_den")  as  "interfreq_hosr"  ,
        sum("ifo_success_rate_nom")|||sum("ifo_success_rate_den")  as  "ifo_success_rate"
        from dt
        group by "date_id"
        ;
    `

    }

    if (tableName === 'dc_e_erbs_eutrancellfdd_v_day') {

        return await sql`
            with dt as (
                select * from dnb.daily_stats.dc_e_erbs_eutrancellfdd_v_day as t1
                    where eutrancellfdd = ${cellId}
            )
            select date_id,
            sum("resource_block_utilizing_rate(dl)_nom")|||sum("resource_block_utilizing_rate(dl)_den")  as  "resource_block_utilizing_rate(dl)" ,
            sum("resource_block_utilizing_rate(ul)_nom")|||sum("resource_block_utilizing_rate(ul)_den")  as  "resource_block_utilizing_rate(ul)" ,
            sum("average_cqi_nom")|||sum("average_cqi_den")  as  "average_cqi" ,
            sum("avg_pusch_ul_rssi_nom")|||sum("avg_pusch_ul_rssi_den")  as  "avg_pusch_ul_rssi"from dt
            group by "date_id"
            ;
        `

    }

}


const getRegionDailyStatsNR = async (tableName) => {

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

const getRegionDailyStatsNRFlex = async (tableName) => {

    if (tableName === 'dc_e_nr_events_nrcellcu_flex_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_nrcellcu_flex as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_events_nrcelldu_flex_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_nrcelldu_flex as dt order by date_id
        `;
    }


    throw new Error('Invalid table name for NR region daily stats; ' + tableName);

}

const getRegionDailyStatsLTEFlex = async (tableName) => {
    if (tableName === 'dc_e_erbs_eutrancellfdd_flex_day') {
        return await sql`
            select * from dnb.daily_stats.kpi_eutrancellfdd_flex as dt order by date_id
        `;
    }

    throw new Error('Invalid table name for LTE region daily stats; ' + tableName);
};

const getCellsList = async (tech, region, cellPartial) => {

    const cellSubstring = cellPartial === '' ? '%' : '%' + cellPartial + '%'

    if (region === 'ALL') {
        return await sql`
                select cell_id from dnb.daily_stats.cells_list
                where cell_id ilike ${cellSubstring} and tech = ${tech}
                order by cell_id LIMIT 200;
                `;
    }
    return await sql`
            select cell_id from dnb.daily_stats.cells_list
            where region ilike ${region} AND cell_id ilike ${cellSubstring} and tech = ${tech}
            ORDER BY cell_id LIMIT 200;
            `;

}

const getClustersList = async (tech, region, clusterPartial) => {
    return await sql`
        select "Region" as region,
        "Cluster_ID" as cluster_id
        from dnb.rfdb.cell_mapping
        where "Cluster_ID" not ilike 'unknown'
        group by "Region", "Cluster_ID"
        order by "Cluster_ID";
    `;
}

const refreshMaterializedViews = async () => {
    logger.info('Refreshing materialized views');

    // cells list

    await sql`
    refresh materialized view concurrently dnb.daily_stats.cells_list;
    `;

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
    refresh materialized view concurrently dnb.daily_stats.kpi_eutrancellfdd_flex;
    `;
    await sql`
    refresh materialized view concurrently dnb.daily_stats.kpi_nrcellcu_flex;
    `;
    await sql`
    refresh materialized view concurrently dnb.daily_stats.kpi_nrcelldu_flex;
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
    logger.info('Finished refreshing materialized views');

};

const refreshMaterializedViewsHourly = async () => {
    logger.info('Refreshing hourly materialized views');
    // NR
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

    // LTE
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_erbs_eutrancellfdd;
    `;
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_erbs_eutrancellrelation;
    `;
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_erbs_eutrancellfdd_v;
    `;


    // FLEX
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_eutrancellfdd_flex;
    `;
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_nrcellcu_flex;
    `;
    await sql`
    refresh materialized view concurrently dnb.hourly_stats.kpi_nrcelldu_flex;
    `;
    logger.info('Finished refreshing hourly materialized views');

};


const createCronToRefreshMaterializedViews = async () => {

    logger.info('Creating cron to refresh materialized views');
    await refreshMaterializedViews();
    await refreshMaterializedViewsHourly();
    const cron = require('node-cron');

    cron.schedule('30 3 * * *', async () => {
        await refreshMaterializedViews();
    });

    cron.schedule('06 * * * *', async () => {
        await refreshMaterializedViewsHourly();
    });

}

const getClusterDailyStatsNR = async (tableName, cluster) => {

    if (tableName === 'dc_e_nr_nrcelldu_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_nr_nrcelldu as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_nrcellcu_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_nr_nrcellcu as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_nrcelldu_v_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_nr_nrcelldu_v as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_erbsg2_mpprocessingresource_v_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_erbsg2_mpprocessingresource_v as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_vpp_rpuserplanelink_v_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_vpp_rpuserplanelink_v as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }


    throw new Error('Invalid table name for NR region daily stats; ' + tableName);

}

const getClusterDailyStatsLTE = async (tableName, cluster) => {

    if (tableName === 'dc_e_erbs_eutrancellfdd_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_erbs_eutrancellfdd as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_erbs_eutrancellfdd_v_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_erbs_eutrancellfdd_v as dt
            where cluster_id = ${cluster}
            order by date_id
        `;
    }

    if (tableName === 'dc_e_erbs_eutrancellrelation_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_erbs_eutrancellrelation as dt
            where cluster_id = ${cluster}
            order by date_id
            `;
    }

}


const getClusterDailyStatsNRFlex = async (tableName) => {

    if (tableName === 'dc_e_nr_events_nrcellcu_flex_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_nrcellcu_flex as dt order by date_id
        `;
    }

    if (tableName === 'dc_e_nr_events_nrcelldu_flex_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_nrcelldu_flex as dt order by date_id
        `;
    }


    throw new Error('Invalid table name for NR cluster flex daily stats; ' + tableName);

}

const getClusterDailyStatsLTEFlex = async (tableName) => {
    if (tableName === 'dc_e_erbs_eutrancellfdd_flex_day') {
        return await sql`
            select * from dnb.daily_stats.clusters_kpi_eutrancellfdd_flex as dt order by date_id
        `;
    }

    throw new Error('Invalid table name for LTE cluster flex daily stats; ' + tableName);
};

module.exports = {
    getCellDailyStatsNR,
    getCellDailyStatsLTE,
    getRegionDailyStatsNR,
    getRegionDailyStatsLTE,

    getRegionDailyStatsNRFlex,
    getRegionDailyStatsLTEFlex,

    createCronToRefreshMaterializedViews,
    refreshMaterializedViews,
    refreshMaterializedViewsHourly,

    getCellsList,
    getClustersList,

    getClusterDailyStatsNR,
    getClusterDailyStatsLTE,
    getCellDailyStatsLTE,
    getClusterDailyStatsNRFlex,
    getClusterDailyStatsLTEFlex
}