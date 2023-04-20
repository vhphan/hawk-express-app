const express = require('express');
const app = express();
const bodyParser = require("body-parser");
require('dotenv').config();


const baseUrl = '/node';
global.__basedir = __dirname;

app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));

// const {createProxyMiddleware} = require("http-proxy-middleware");
const { codeProxy } = require('./middleware/proxies');
const errorHandler = require('./middleware/error');

app.use(errorHandler);
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


// const { sendEmail } = require('./utils');

app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
    // sendEmail(process.env.EMAIL_RECIPIENT, 'Server Started', 'Server Started');
    console.log(process.env.NODE_ENV);
    console.log(__dirname);
});

// if in dev mode redirect to /ctr/ctr-dl
// TODO: remove this in production
if (process.env.NODE_ENV === "development") {
    app.get('/', (req, res) => res.redirect('/node/ctr/ctr-dl'));
}

const { createSocket } = require('./ePortal/eSocket');
const { createCronToDeleteFilesOlderThanNDays } = require('./routes/utils');

const socket = createSocket();

createCronToDeleteFilesOlderThanNDays(__dirname + '/tmp/dl', 2);