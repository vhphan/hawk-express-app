const express = require('express');
const app = express();
app.get('/node/', function (req, res) {
    res.send('Hello World!');
});
app.get('/node/hello2', function (req, res) {
    res.send('Hello World2!');
});
app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
});