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
    getSitesList,
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
const { cache6h } = require("../middleware/cache");
const { tables, tablesHourly, flexTables, flexTablesHourly } = require("./tables");
const { compileResultsKpiArrays, compileResultsKpiArraysFlex } = require("./kpiUtils");[]

// log full url for every request
router.use((req, res, next) => {
    logger.info(req.protocol + '://' + req.get('host') + req.originalUrl);
    next();
});

router.get("/test", testQuery);



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

router.get('/sitesList', asyncHandler(async (req, res) => {

    const tech = req.query.tech || 'nr';
    const region = req.query.region || 'all';
    const sitePartial = req.query.sitePartial || ''

    const results = await getSitesList(tech, region, sitePartial);
    res.json({
        success: true,
        data: results,
        meta: {
            time: new Date(),
            region,
            tech,
            sitePartial,
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
        if (tech === 'nr') return getClusterDailyStatsNRFlex(table, cluster)
        if (tech === 'lte') return getClusterDailyStatsLTEFlex(table, cluster)
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
        if (tech === 'nr') return getClusterHourlyStatsNRFlex(table, cluster)
        if (tech === 'lte') return getClusterHourlyStatsLTEFlex(table, cluster)
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

module.exports = router;



