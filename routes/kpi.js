const express = require("express");
const router = express.Router();


const { testQuery } = require("../db/dailyKpiQueries");
const { getCellDailyStatsNR, getRegionDailyStatsNR } = require("../controllers/kpi");
const asyncHandler = require("../middleware/async");
const { kpiList } = require("./configs/kpiList");


const tables = {

    nr: [
        'dc_e_nr_nrcelldu_day',
        'dc_e_nr_nrcellcu_day',
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

    const results = await compileResults(promises, tech, res);

    res.json({
        success: true,
        data: results,
        time: new Date(),
    });

}));

router.get('/dailyStatsRegion', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    let { region } = req.query;

    const promises = tables[tech].map((table) =>
        getRegionDailyStatsNR(table)
    )

    if (!region) { region = 'all' }

    const results = await compileResultsKpiArrays(promises, tech, region);

    res.json({
        success: true,
        data: results,
        time: new Date(),
        params: {
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

async function compileResultsKpiArrays(promises, tech, region) {

    const byKpiResultsArrays = {};
    const kpiListTech = kpiList[tech];
    const results = await Promise.all(promises);

    // filter by region
    const regionResults = results.map(result => result.filter(d => {
        if (region.toLowerCase() === 'all') return d.region === null;
        return d.region === region
    }));

    const flattenedResults = regionResults.flat();

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

