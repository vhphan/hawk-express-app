const express = require("express");
const router = express.Router();
const { logger } = require('../middleware/logger');


const { testQuery } = require("../db/dailyKpiQueries");
const {

    getCellDailyStatsNR,
    getCellDailyStatsLTE,
    getRegionDailyStatsNR,
    getRegionDailyStatsLTE,
    getRegionDailyStatsNRFlex,
    getRegionDailyStatsLTEFlex,
    getCellsList,
    getClustersList,
    getClusterDailyStatsNR,
    getClusterDailyStatsLTE,
    getClusterDailyStatsNRFlex,
    getClusterDailyStatsLTEFlex

} = require("../controllers/kpi");
const {

    getRegionHourlyStatsNR,
    getRegionHourlyStatsLTE,
    getRegionHourlyStatsNRFlex,
    getRegionHourlyStatsLTEFlex,
    getCellHourlyStatsNR,
    getCellHourlyStatsLTE,
    getClusterHourlyStatsNR,
    getClusterHourlyStatsLTE,
    getClusterHourlyStatsNRFlex,
    getClusterHourlyStatsLTEFlex

} = require("../controllers/kpiHourly");
const asyncHandler = require("../middleware/async");
const { kpiList, kpiListFlex, mobileOperators } = require("../configs/kpiList");
const { cache6h } = require("../middleware/cache");

// log full url for every request
router.use((req, res, next) => {
    logger.info(req.protocol + '://' + req.get('host') + req.originalUrl);
    next();
});

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
}

const flexTablesHourly = {
    nr: [
        'dc_e_nr_events_nrcellcu_flex_raw',
        'dc_e_nr_events_nrcelldu_flex_raw',
    ],
    lte: [
        'dc_e_erbs_eutrancellfdd_flex_raw',
    ]
};

router.get("/test", testQuery);

router.get('/dailyStats', asyncHandler(async (req, res) => {

    const { tech, cellId } = req.query;
    const getCellStatsTechFunc = tech === 'nr' ? getCellDailyStatsNR : getCellDailyStatsLTE;
    const promises = tables[tech].map((table) =>
        getCellStatsTechFunc(cellId, table)
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
    const region = req.query.region || 'all';

    const promises = tables[tech].map((table) => {
        if (tech === 'nr') {
            return getRegionDailyStatsNR(table);
        }
        if (tech === 'lte') {
            return getRegionDailyStatsLTE(table);
        }
    })

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
    const region = req.query.region || 'all';

    const promises = tablesHourly[tech].map((table) => {

        if (tech === 'nr') return getRegionHourlyStatsNR(table);
        if (tech === 'lte') return getRegionHourlyStatsLTE(table);

    });


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

router.get('/dailyStatsRegionFlex', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    const region = req.query.region || 'all';

    const promises = flexTables[tech].map((table) => {
        if (tech === 'nr') return getRegionDailyStatsNRFlex(table)
        if (tech === 'lte') return getRegionDailyStatsLTEFlex(table)
    });

    const results = await compileResultsKpiArraysFlex(promises, tech, region, 'region');

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

router.get('/hourlyStatsRegionFlex', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    const region = req.query.region || 'all';

    const promises = flexTablesHourly[tech].map((table) => {
        if (tech === 'nr') return getRegionHourlyStatsNRFlex(table)
        if (tech === 'lte') return getRegionHourlyStatsLTEFlex(table)
    });

    const results = await compileResultsKpiArraysFlex(promises, tech, region, 'region');

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

router.get('/cellsList', asyncHandler(async (req, res) => {

    const tech = req.query.tech || 'nr';
    const region = req.query.region || 'all';
    const cellPartial = req.query.cellPartial || ''

    const results = await getCellsList(tech, region, cellPartial);
    res.json({

        success: true,
        data: results,
        meta: {
            time: new Date(),
            region,
            tech,
            cellPartial,
        }
    })

}));

router.get('/clustersList', cache6h, asyncHandler(async (req, res) => {

    const results = await getClustersList();
    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
        }
    })

}));

router.get('/dailyStatsCluster', asyncHandler(async (req, res) => {

    const { tech } = req.query;
    const cluster = req.query.cluster;

    const promises = tables[tech].map((table) => {
        if (tech === 'nr') return getClusterDailyStatsNR(table, cluster);
        if (tech === 'lte') return getClusterDailyStatsLTE(table, cluster);
    });

    const results = await compileResultsKpiArrays(promises, tech, cluster, 'cluster');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            cluster,
            tech,
        }
    });


}));



router.get('/hourlyStatsCluster', asyncHandler(async (req, res) => {

    const { tech, cluster } = req.query;

    const promises = tablesHourly[tech].map((table) => {

        if (tech === 'nr') return getClusterHourlyStatsNR(table, cluster);
        if (tech === 'lte') return getClusterHourlyStatsLTE(table, cluster);

    });


    const results = await compileResultsKpiArrays(promises, tech, cluster, 'cluster');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            cluster,
            tech,
        }
    });


}));


router.get('/dailyStatsClusterFlex', asyncHandler(async (req, res) => {

    const { tech, cluster } = req.query;

    const promises = flexTables[tech].map((table) => {
        if (tech === 'nr') return getClusterDailyStatsNRFlex(table)
        if (tech === 'lte') return getClusterDailyStatsLTEFlex(table)
    });

    const results = await compileResultsKpiArraysFlex(promises, tech, cluster, 'cluster');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            cluster,
            tech,
        }
    });

}));


router.get('/hourlyStatsClusterFlex', asyncHandler(async (req, res) => {

    const { tech, cluster } = req.query;

    const promises = flexTablesHourly[tech].map((table) => {
        if (tech === 'nr') return getClusterHourlyStatsNRFlex(table)
        if (tech === 'lte') return getClusterHourlyStatsLTEFlex(table)
    });

    const results = await compileResultsKpiArraysFlex(promises, tech, cluster, 'cluster');

    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            cluster,
            tech,
        }
    });

}));


module.exports = router;





//---------------------------------------------------------------------------------------------------------------------------
// functions
//---------------------------------------------------------------------------------------------------------------------------
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

async function compileResultsKpiArraysFlex(promises, tech, region, level = 'region') {

    const kpiListTech = kpiListFlex[tech];
    const results = await Promise.all(promises);
    const flattenedResults = getFlattenedResults(level, results, region);
    const finalResults = {};

    kpiListTech.forEach(kpi => {

        mobileOperators.forEach(operator => {

            const seriesData = flattenedResults
                .filter(d => Object.keys(d).includes(kpi))
                .filter(d => d['mobile_operator'] === operator)
                .map(d => (
                    [
                        d.date_id,
                        d[kpi],
                    ]
                ));
            if (!finalResults[kpi]) finalResults[kpi] = [];
            finalResults[kpi].push({
                name: operator,
                data: seriesData,
            })

        });

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




