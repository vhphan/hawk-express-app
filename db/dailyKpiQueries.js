const sql = require('./PgBackend');

const testQuery = async (request, response) => {
    const results = await sql`SELECT * FROM dnb.daily_stats.dc_e_nr_nrcelldu_day LIMIT 5`;
    response.status(200).json(results);
}

module.exports = {
    testQuery
}