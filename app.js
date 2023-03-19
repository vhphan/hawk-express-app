const express = require('express');
const app = express();
const baseUrl = '/node';
global.__basedir = __dirname;

// const {createProxyMiddleware} = require("http-proxy-middleware");
const { codeProxy } = require('./middleware/proxies');
app.use(baseUrl + '/main', require('./routes/main'));
app.use(baseUrl + '/ctr', require('./routes/ctr'));
app.get('/node/', function (req, res) {
    res.send('Hello World!');
});

// app.use('/node/vscode', createProxyMiddleware({
//         changeOrigin: true,
//         prependPath: false,
//         target: "http://localhost:8000",
//         ws: true,
//         logLevel: 'debug',
//         pathRewrite: {
//             '^/node/vscode': '', // remove base path
//         },
//     })
// );


app.get('/node/bootstrap.bundle.min.js', (req, res) => res.sendFile(__dirname + '/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js'));
app.get('/node/bootstrap.min.css', (req, res) => res.sendFile(__dirname + '/node_modules/bootstrap/dist/css/bootstrap.min.css'));

app.use('/node/vscode/**', codeProxy);

app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
});