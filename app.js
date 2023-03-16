const express = require('express');
const app = express();
const baseUrl = '/node';
const {createProxyMiddleware} = require("http-proxy-middleware");

app.use(baseUrl + '/main', require('./routes/main'));
app.get('/node/', function (req, res) {
    res.send('Hello World!');
});

app.use('/node/vscode', createProxyMiddleware({
        changeOrigin: true,
        prependPath: false,
        target: "http://localhost:8000",
        ws: true,
        logLevel: 'debug',
        pathRewrite: {
            '^/node/vscode': '', // remove base path
        },
    })
);


app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
});