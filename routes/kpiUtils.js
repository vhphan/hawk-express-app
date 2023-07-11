const { kpiList, kpiListFlex, mobileOperators } = require("../configs/kpiList");
// const { tables } = require("./tables");

// async function compileResults(promises, tech) {
//     const results = await Promise.all(promises);

//     // zip tables and results with table as key
//     const zipped = tables[tech].reduce((acc, table, index) => {
//         acc[table] = results[index];
//         return acc;
//     }, {});

//     return zipped;
// }

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
            });

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

module.exports = {
    // compileResults,
    compileResultsKpiArrays,
    compileResultsKpiArraysFlex,
};
