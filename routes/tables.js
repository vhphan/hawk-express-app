const tables = {
    nr: [
        'dc_e_nr_nrcelldu_day',
        'dc_e_nr_nrcellcu_day',
        'dc_e_nr_nrcelldu_v_day',
        'dc_e_erbsg2_mpprocessingresource_v_day',
        'dc_e_vpp_rpuserplanelink_v_day',
    ],
    lte: [
        'dc_e_erbs_eutrancellfdd_day',
        'dc_e_erbs_eutrancellrelation_day',
        'dc_e_erbs_eutrancellfdd_v_day'
    ]
};
const flexTables = {
    nr: [
        'dc_e_nr_events_nrcellcu_flex_day',
        'dc_e_nr_events_nrcelldu_flex_day',
    ],
    lte: [
        'dc_e_erbs_eutrancellfdd_flex_day',
    ]
};
const tablesHourly = {
    nr: [
        'dc_e_nr_nrcelldu_raw',
        'dc_e_nr_nrcellcu_raw',
        'dc_e_nr_nrcelldu_v_raw',
        'dc_e_erbsg2_mpprocessingresource_v_raw',
        'dc_e_vpp_rpuserplanelink_v_raw',
    ],
    lte: [
        'dc_e_erbs_eutrancellfdd_raw',
        'dc_e_erbs_eutrancellrelation_raw',
        'dc_e_erbs_eutrancellfdd_v_raw'
    ]
};
const flexTablesHourly = {
    nr: [
        'dc_e_nr_events_nrcellcu_flex_raw',
        'dc_e_nr_events_nrcelldu_flex_raw',
    ],
    lte: [
        'dc_e_erbs_eutrancellfdd_flex_raw',
    ]
};

module.exports = {
    tables,
    flexTables,
    tablesHourly,
    flexTablesHourly
};