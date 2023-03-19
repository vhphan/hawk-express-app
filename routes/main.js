
const express = require('express');
const {createProxyMiddleware} = require("http-proxy-middleware");
const router = express.Router();

router.get('/hello', function (req, res) {
    console.log(req.query);
    res.send('Hello from main');
});



// router.all('/vscode/', codeProxy);
// router.all('/vscode/*', codeProxy);


module.exports = router;

