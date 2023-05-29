const kpiList = {

    nr: [
        "cell_availability",
        "gnodeb_cpu_load",
        "resource_block_utilizing_rate_dl",
        "avg_pusch_ul_rssi",
        "ul_bler",
        "intra-sgnb_pscell_change_success_rate",
        "ul_64qam",
        "ul_16qam",
        "ul_256qam",
        "dl_data_volume_gb",
        "ul_qpsk",
        "average_cqi",
        "dl_256qam",
        "dl_mac_vol_as_scell_ext",
        "ul_data_volume_gb",
        "inter-sgnb_pscell_change_success_rate",
        "dl_16qam",
        "endc_ca_deconfiguration_sr",
        "erab_drop_call_rate_sgnb",
        "rrc_setup_success_rate_signaling",
        "dl_64qam",
        "dl_user_throughput",
        "packet_loss_ul",
        "dl_mac_vol_to_scell",
        "ul_user_throughput",
        "dl_cell_throughput",
        "dl_mac_vol_as_scell",
        "total_traffic_gb",
        "ul_cell_throughput",
        "endc_sr",
        "resource_block_utilizing_rate_ul",
        "packet_loss_dl",
        "latency_only_radio_interface",
        "dl_qpsk",
        "e-rab_block_rate",
        "dl_mac_vol_to_scell_ext",
        "endc_ca_configuration_sr"
    ],

    lte: [
        "interfreq_hosr",
        "ul_bler",
        "ul_user_throughput",
        "dl_qpsk",
        "rrc_setup_success_rate_(service)",
        "ul_qpsk",
        "rrc_setup_success_rate_(signaling)",
        "packet_loss_(dl)",
        "intrafreq_hosr",
        "ifo_success_rate",
        "total_traffic",
        "average_cqi",
        "call_setup_success_rate",
        "dl_cell_throughput",
        "dl_data_volume",
        "packet_loss_(ul)",
        "ifo_success_rate",
        "e-rab_setup_success_rate_non_gbr",
        "handover_in_success_rate",
        "dl_user_throughput",
        "dl_64qam",
        "cell_availability",
        "avg_pusch_ul_rssi",
        "interfreq_hosr",
        "volte_redirection_success_rate",
        "erab_drop_call_rate",
        "dl_256qam",
        "ul_256qam",
        "ul_cell_throughput",
        "resource_block_utilizing_rate(ul)",
        "ul_data_volume",
        "e-rab_setup_success_rate",
        "ul_64qam",
        "resource_block_utilizing_rate(dl)",
        "ul_16qam",
        "dl_16qam",
        "latency_(only_radio_interface)"
    ]

}

const kpiListFlex = {

    nr: [
        "5g_ho_success_rate_dnb_5g_to_dnb",
        "dl_16qam",
        "dl_64qam",
        "dl_256qam",
        "dl_qpsk",
        "dl_user_throughput",
        "inter-sgnb_pscell_change_success",
        "inter_rat_ho_success_rate_dnb_5g_to_mno_4g",
        "intra-sgnb_pscell_change_success",
        "ul_16qam",
        "ul_64qam",
        "ul_qpsk",
        "ul_traffic_volume",
        "ul_user_throughput"
    ],

    lte: [
        "dl_16qam", 
        "dl_64qam", 
        "dl_256qam", 
        "dl_cell_throughput", 
        "dl_data_volume", 
        "dl_qpsk", 
        "dl_user_throughput", 
        "e-rab_setup_success_rate", 
        "erab_drop_call_rate", 
        "intrafreq_hosr", 
        "packet_loss_(dl)", 
        "packet_loss_(ul)", 
        "ul_16qam", 
        "ul_64qam", 
        "ul_256qam", 
        "ul_bler", 
        "ul_cell_throughput", 
        "ul_data_volume", 
        "ul_qpsk", 
        "ul_user_throughput"
    ]
}

mobileOperators = [
    
    'Celcom',
    'Digi',
    'Maxis',
    'TM',
    'Umobile',
    'YTL',

]

module.exports = {
    kpiList,
    kpiListFlex,
    mobileOperators
};