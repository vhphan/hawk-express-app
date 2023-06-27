const express = require('express');

const app = express();
const cors = require('cors');
const bodyParser = require("body-parser");
require('dotenv').config();

const { logger } = require('./middleware/logger');


const baseUrl = '/node';
global.__basedir = __dirname;

app.use(cors());
app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));

// const {createProxyMiddleware} = require("http-proxy-middleware");
const { codeProxy } = require('./middleware/proxies');
const errorHandler = require('./middleware/error');


app.use(baseUrl + '/main', require('./routes/main'));
app.use(baseUrl + '/ctr', require('./routes/ctr'));
app.use(baseUrl + '/kpi/v1', require('./routes/kpi'));

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


// if in dev mode redirect to /ctr/ctr-dl
// TODO: remove this in production
if (process.env.NODE_ENV === "development") {
    app.get('/', (req, res) => res.redirect('/node/ctr/ctr-dl'));
}

app.use('/node/apps/dashboard-v1/', express.static(__dirname + '/apps/kpi-frontend/'));
app.use('/node/apps/', express.static(__dirname + '/apps/'));
const kpiFrontendRoute = (req, res) => res.sendFile(__dirname + '/apps/dashboard-v1/index.html');
app.get('/node/apps/dashboard-v1', kpiFrontendRoute);
app.get('/node/apps/dashboard-v1/*', kpiFrontendRoute);



const { createSocket } = require('./ePortal/eSocket');
const { createCronToDeleteFilesOlderThanNDays } = require('./routes/utils');
const { createCronToRunMainDPM, createCronToRunMainCellMapping } = require('./controllers/dnb');
const { createCronToRefreshMaterializedViews } = require('./controllers/kpi');

const socket = createSocket();


if (process.env.NODE_ENV !== "development") {
    try {
        createCronToDeleteFilesOlderThanNDays(__dirname + '/tmp/dl', 2);
        createCronToRunMainDPM();
        createCronToRunMainCellMapping();
        createCronToRefreshMaterializedViews();
    } catch (error) {
        logger.error(error);
    }
}

if (process.env.NODE_ENV === "development") {

}


app.use(errorHandler);

// const { sendEmail } = require('./utils');


app.listen(3000, function () {
    logger.info(__dirname);
    console.log('Example app listening on port 3000!');
    // sendEmail(process.env.EMAIL_RECIPIENT, 'Server Started', 'Server Started');
    console.log('process.env.NODE_ENV', process.env.NODE_ENV);
    logger.info(`Server Started in ${process.env.NODE_ENV} mode`)
    console.log(__dirname);
});