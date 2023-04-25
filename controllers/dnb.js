
const sql = require('../db/PgBackend');
const axios = require('axios');
require('dotenv').config('../.env');

const updateDfDpmTable = async () => {

    const url = new URL('https://api.eprojecttrackers.com/node/dnb/tabulatorData');

    const params = {
        table: 'df_dpm',
        page: 1,
        size: 1000,
        schema: 'rfdb'
    }

    url.searchParams.append('table', params.table);
    url.searchParams.append('page', params.page);
    url.searchParams.append('size', params.size);
    url.searchParams.append('schema', params.schema);

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

    const allData = data;
    const promises = [];
    for (let i = 2; i <= last_page; i++) {
        const url = new URL('https://api.eprojecttrackers.com/node/dnb/tabulatorData');
        url.searchParams.append('table', params.table);
        url.searchParams.append('page', i);
        url.searchParams.append('size', params.size);
        url.searchParams.append('schema', params.schema);

        promises.push(axios.get(url.href, {
            headers: {
                api: api
            }
        }));
    }

    const responseArray = await Promise.all(promises);
    responseArray.forEach((response) => {
        allData.push(...response.data.data);
    });

    return allData;

}


