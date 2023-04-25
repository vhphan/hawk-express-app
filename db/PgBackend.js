// postgres backend using postgres.js

const postgres = require('postgres');

const sql = postgres({
    host: 'localhost',
    port: 5432,
    database: 'dnb',
    username: process.env.PGDB_USER,
    password: process.env.PGDB_PASSWORD,
})

module.exports = sql