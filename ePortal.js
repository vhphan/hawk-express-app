
// var socket = client.connect("http://myendpoint.com:3000/whatever");

const createSocketClient = () => {
    console.log("Creating socket client");
    const client = require("socket.io-client");
    const theServer = 'https://api.eprojecttrackers.com';
    const socket = client.connect(theServer + "/node/dnbSocket/socket");
    socket.on("connect", () => {
        console.log(socket.connected); // true
    });

    socket.on("disconnect", () => {
        console.log(socket.connected); // false
    });

    socket.emit("test", "foo");
    return socket;
}

module.exports = {
    createSocketClient,
}