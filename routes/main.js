const express = require('express');
const {createProxyMiddleware} = require("http-proxy-middleware");
const router = express.Router();

router.get('/hello', function (req, res) {
    res.send('Hello from main');
});

let codeProxy = createProxyMiddleware({
    changeOrigin: true,
    prependPath: false,
    target: "http://127.0.0.1:8000/",
    logLevel: 'debug',
    ws: true,
    pathRewrite: {
        '^/node/main/vscode': '', // remove base path
    },
});

router.all('/vscode/*', codeProxy);


module.exports = router;

