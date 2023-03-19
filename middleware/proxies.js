const { createProxyMiddleware } = require("http-proxy-middleware");

let codeProxy = createProxyMiddleware({
  changeOrigin: true,
  prependPath: false,
  target: "http://127.0.0.1:8000/",
  logLevel: "debug",
  ws: true,
  //    pathRewrite: {
  //       '^/node/main/vscode': '/node/main/vscode', // remove base path
  //    },
});

module.exports = { codeProxy };
