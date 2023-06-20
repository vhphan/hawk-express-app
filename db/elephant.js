const postgres = require('postgres');
require('dotenv').config();

const sqlElephant = postgres({
    host: process.env.ELEPHANT_HOST,
    port: 5432,
    database: process.env.ELEPHANT_DB,
    username: process.env.ELEPHANT_USER,
    password: process.env.ELEPHANT_PASSWORD,
})

module.exports = sqlElephant