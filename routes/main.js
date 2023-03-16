const express = require('express');
const main = express.Router();

main.get('/hello', function (req, res) {
    res.send('Hello from main');
});




module.exports = main;

