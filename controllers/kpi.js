const sql = require('../db/PgBackend');



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
        sum("e-rab_block_rate_nom")|||sum("e-rab_block_rate_den")  as  "e-rab_block_rate" ,
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
            sum("endc_ca_deconfiguration_sr_nom")|||sum("endc_ca_deconfiguration_sr_den")  as  "endc_ca_deconfiguration_sr"
        from
        dnb.daily_stats.dc_e_nr_nrcellcu_day as dt
        WHERE nrcellcu=${cellId}
        GROUP BY date_id, nrcellcu;



        `;
    }


}

const getRegionDailyStatsLTE = async (tableName) => {

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

    throw new Error('Invalid table name for NR region daily stats; ' + tableName);

}

    


module.exports = {
    getCellDailyStatsNR,
    getRegionDailyStatsNR
}