const express = require("express");
const router = express.Router();


const { testQuery } = require("../db/dailyKpiQueries");
const { 
    getCellDailyStatsNR, 
    getCellDailyStatsLTE,
    getRegionDailyStatsNR,
    getRegionDailyStatsLTE,

} = require("../controllers/kpi");
const { getRegionHourlyStatsNR, getCellHourlyStatsN } = require("../controllers/kpiHourly");
const asyncHandler = require("../middleware/async");
const { kpiList } = require("../configs/kpiList");


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
}

const tablesHourly = {
    nr: [
        'dc_e_nr_nrcelldu_raw',
        'dc_e_nr_nrcellcu_raw',
        'dc_e_nr_nrcelldu_v_raw',
        'dc_e_erbsg2_mpprocessingresource_v_raw',
        'dc_e_vpp_rpuserplanelink_v_raw',
    ],
    lte: [


    ]
}



router.get("/test", testQuery);

router.get('/dailyStats', asyncHandler(async (req, res) => {

    const { tech, cellId } = req.query;
    const promises = tables[tech].map((table) =>
        getCellDailyStatsNR(cellId, table)
    )
    const results = await compileResultsKpiArrays(promises, tech, null, 'cell');

    res.json({
        success: true,
        data: results,
        time: new Date(),
    });

}));

router.get('/dailyStatsRegion', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    let { region } = req.query;

    const promises = tables[tech].map((table) =>{
        if (tech==='nr'){
            return getRegionDailyStatsNR(table);
        }
        if (tech ==='lte'){
            return getRegionDailyStatsLTE(table);
        }
    })

    if (!region) { region = 'all' }

    const results = await compileResultsKpiArrays(promises, tech, region, 'region');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            region,
            tech,
        }
    });

}));


router.get('/hourlyStatsRegion', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    let { region } = req.query;

    const promises = tablesHourly[tech].map((table) =>
        getRegionHourlyStatsNR(table)
    );

    if (!region) { region = 'all' }

    const results = await compileResultsKpiArrays(promises, tech, region, 'region');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            region,
            tech,
        }
    });


}));

module.exports = router;


// functions
// compile results from promises
async function compileResults(promises, tech) {
    const results = await Promise.all(promises);

    // zip tables and results with table as key
    const zipped = tables[tech].reduce((acc, table, index) => {
        acc[table] = results[index];
        return acc;
    }, {});

    return zipped;
}


async function compileResultsKpiArrays(promises, tech, region, level = 'region') {

    const kpiListTech = kpiList[tech];
    const results = await Promise.all(promises);
    const flattenedResults = getFlattenedResults(level, results, region);
    const finalResults = {};

    kpiListTech.forEach(kpi => {
        finalResults[kpi] = flattenedResults.filter(d => Object.keys(d).includes(kpi)).map(d => (
            [
                d.date_id,
                d[kpi],
            ]
        ));
    });

    return finalResults;

}

function getFlattenedResults(level, results, region) {
    if (level === 'region') {
        const regionResults = results.map(result => result.filter(d => {
            if (region.toLowerCase() === 'all')
                return d.region === null;
            return d.region === region.toUpperCase();
        }));
        return regionResults.flat();
    }
    return results.flat();
}

