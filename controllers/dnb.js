
const sql = require('../db/PgBackend');
const axios = require('axios');
const { logger } = require('../middleware/logger');



require('dotenv').config('../.env');


const getSortersForTable = (tableName) => {
    let sorters = [];
    switch (tableName) {
        case 'df_dpm':
            sorters = [
                {
                    "field": "dnb_index",
                    "dir": "asc"
                }
            ];
            break;
        case 'cell_mapping':
            sorters = [
                {
                    "field": "Cellname",
                    "dir": "asc"
                }
            ];
            break;
        };
    return sorters;
}



const getTableDataFromPortal = async (tableName, schema = 'rfdb') => {

    const url = new URL('https://api.eprojecttrackers.com/node/dnb/tabulatorData');

    let sorters = getSortersForTable(tableName);

    const params = {
        table: tableName,
        page: 1,
        size: 1000,
        schema: schema,
    }

    url.searchParams.append('table', params.table);
    url.searchParams.append('page', params.page);
    url.searchParams.append('size', params.size);
    url.searchParams.append('schema', params.schema);

    // sorters[0][field]=WorkPlanID&sorters[0][dir]=asc
    sorters.forEach((sorter, index) => {
        url.searchParams.append(`sorters[${index}][field]`, sorter.field);
        url.searchParams.append(`sorters[${index}][dir]`, sorter.dir);
    });


    const api = process.env.EPORTAL_API_KEY;

    const response = (await axios.get(url.href, {
        headers: {
            API: api
        }
    })
    ).data;


    const { data, last_page, success } = response;

    if (!success) {
        console.log('Error in fetching data from ePortal');
        throw new Error('Error in fetching data from ePortal');
    }
    logger.info(`Fetched ${data.length} rows from ${tableName} table`);

    const allData = data;
    const promises = [];
    for (let i = 2; i <= last_page; i++) {
        const url = new URL('https://api.eprojecttrackers.com/node/dnb/tabulatorData');
        url.searchParams.append('table', params.table);
        url.searchParams.append('page', i);
        url.searchParams.append('size', params.size);
        url.searchParams.append('schema', params.schema);
        sorters.forEach((sorter, index) => {
            url.searchParams.append(`sorters[${index}][field]`, sorter.field);
            url.searchParams.append(`sorters[${index}][dir]`, sorter.dir);
        });
        
        promises.push(axios.get(url.href, {
            headers: {
                api: api
            }
        }));
    }

    const responseArray = await Promise.all(promises);
    responseArray.forEach((response) => {
        allData.push(...response.data.data);
        logger.info(`Fetched ${response.data.data.length} rows from ${tableName} table`);
    });

    // save allData to json file
    const fs = require('fs');
    // get date time in format: 2021-01-01_00-00-00
    const date = new Date();
    const dateTime = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate() + '_' + date.getHours() + '-' + date.getMinutes() + '-' + date.getSeconds();
    fs.writeFileSync(`/home/hawkuser/data2/var/www/hawk-express-app/db/df_dpm/${tableName}_${dateTime}.json`, JSON.stringify(allData));

    return allData;

}

async function insertRow(sql_, rowData, tableName, schemaName) {
    /*
    sql`
    insert into users ${
    sql(user, 'name', 'age')
    }
    `
    */
    let result;
    switch (tableName) {
        case 'df_dpm':
            result = await sql_`
        insert into ${sql(schemaName)}.df_dpm ${sql(rowData, 'dnb_index', 'nominal_id', '_id', "Nominal_ID", "Site_Name", "Latitude", "Longitude", 'site_id', "Nominal_Latitude", "Nominal_Longitude", "Candidate_Latitude", "Candidate_Longitude", 'on_board_date', 'api_call_date', 'added', "Acceptance_Cluster", "Sub_Cluster", "CBOClusterName")}
        `;
            break;
        case 'cell_mapping':
            result = await sql_`
        insert into ${sql(schemaName)}.cell_mapping ${sql(rowData, "Cellname", "Region", "Cluster_ID", "DISTRICT", "MCMC_State", "geom", "SITEID", "Sitename")}
        `;
            break;

        default:
            throw new Error('Invalid table name');
    }
    return result;

}


async function insertMultipleRows(rows, tableName, schemaName) {

    const result = await sql.begin(async sql => {
        rows.map(async (row) => {
            await insertRow(sql, row, tableName, schemaName);
        });
    })

    return result;

}

const deleteAllRows = async (tableName, schemaName) => {
    const result = await sql`
    delete from ${sql(schemaName)}.${sql(tableName)}
        where true
    `;
    logger.info(`Deleted all rows from ${tableName} table`);
    return result;
};

const insertDfDpmTable = async () => {
    deleteAllRows('df_dpm', 'rfdb');
    const data = await getTableDataFromPortal('df_dpm');
    const result = await insertMultipleRows(data, 'df_dpm', 'rfdb');
    console.log(result);
    return result;
};

const insertCellMappingTable = async () => {
    deleteAllRows('cell_mapping', 'rfdb');
    const data = await getTableDataFromPortal('cell_mapping');
    const result = await insertMultipleRows(data, 'cell_mapping', 'rfdb');
    console.log(result);
    return result;
};


const updateMetaTable = async (tableName) => {
    const result = await sql`
        insert into daily_stats.meta(table_name, last_updated) values(${tableName}, now())
        on conflict(table_name) do update set last_updated = now()
        `;
    logger.info('Updated meta table');
    return result;
}

const mainDpm = () => {

    try {
        insertDfDpmTable();
    } catch (error) {
        logger.error(error.message || 'Error in mainDpm function');
    }
    // updateMetaTable('df_dpm');
}

const createCronToRunMainDPM = () => {
    const cron = require('node-cron');
    cron.schedule('0 0 2 * * *', () => {
        mainDpm();
    });
}

const mainCellMapping = () => {
    try {
        insertCellMappingTable();
    } catch (error) {
        logger.error(error.message || 'Error in mainCellMapping function');
    }
}

const createCronToRunMainCellMapping = () => {
    const cron = require('node-cron');
    cron.schedule('0 0 3 * * *', () => {
        mainCellMapping();
    });
}


module.exports = {
    mainDpm,
    createCronToRunMainDPM,
    mainCellMapping,
    createCronToRunMainCellMapping
}

