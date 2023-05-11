const io = require('socket.io-client');

const currentServer = 'https://api.eprojecttrackers.com';
const currentLocalDevServer = 'http://localhost:3000';

const { logger } = require('../middleware/logger');

function runShellCommands(shellCommands, execSync) {
    const shellOutputs = [];
    shellCommands.forEach((command) => {
        logger.info(`executing command: ${command}`);
        shellOutputs.push(execSync(command).toString());
    }
    );
    return shellOutputs;
}

const createSocket = () => {

    const socket = io.connect(currentServer, { path: "/node/dnbSocket/socket" });
    socket.on("connect", () => {
        logger.info(socket.connected); // true
    });

    const reconnect = (reconnectMinutes = 5) => {
        setTimeout(() => {
            // check if socket is connected
            if (!socket.connected) {
                logger.info('reconnecting');
                createSocket();
            }
        }
            , reconnectMinutes * 60 * 1000);
    }

    socket.on("disconnect", () => {
        logger.info(socket.connected); // false
        // retry connection in 5 min
        logger.info('retrying connection in 5 min');
        reconnect(5);
    });

    // check if socket has error while connecting
    socket.on("error", (error) => {
        logger.error(error);
        reconnect(5);
    });


    socket.on("ePortalToDnb", function (data) {

        logger.info(data);
        let shellOutputs = [];
        const { execSync } = require("child_process");

        if (data.split(' ').at(0) === 'resetCodeTunnel' && process.env.NODE_ENV === 'production') {
            logger.info('resetting code tunnel');
            const shellCommands = [
                'tmux send-keys -t code-tunnel C-c',
                'tmux send-keys -t code-tunnel "./code tunnel"',
                'tmux send-keys -t code-tunnel Enter',
                // 'kill -9 $(pgrep -f nodemon)',
                // 'kill $(lsof -t -i:3000)'
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }
        
        if (data.split(' ').at(0) === 'resetCodeTunnel2' && process.env.NODE_ENV === 'production') {
            logger.info('resetting code tunnel 2');
            const shellCommands = [
                'tmux send-keys -t monica C-c',
                'tmux send-keys -t monica "./code tunnel"',
                'tmux send-keys -t monica Enter',
                // 'kill -9 $(pgrep -f nodemon)',
                // 'kill $(lsof -t -i:3000)'
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }

        if (data.split(' ').at(0) === 'startWetty' && process.env.NODE_ENV === 'production') {
            logger.info('starting wetty');
            const shellCommands = [
                'tmux send-keys -t wetty C-c',
                'tmux send-keys -t wetty "npm run start"',
                'tmux send-keys -t wetty Enter',
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }
        if (data.split(' ').at(0) === 'stopWetty' && process.env.NODE_ENV === 'production') {
            logger.info('stopping wetty');
            const shellCommands = [
                'tmux send-keys -t wetty C-c',
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }


        if (data.split(' ').at(0) === 'killCodeTunnel' && process.env.NODE_ENV === 'production') {
            logger.info('killing code tunnel');
            const shellCommands = [
                'tmux send-keys -t code-tunnel C-c',
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }

        logger.info('shellOutputs: ' + shellOutputs);
        socket.emit("ePortalFromDnb", { shellOutputs });


    });

    socket.on("ePortalToDnbCommands", function (data) {
        logger.info(data);
        const { execSync } = require("child_process");
        const shellOutputs = runShellCommands([data], execSync);
        socket.emit("ePortalFromDnb", { shellOutputs });
    });

    if (process.env.NODE_ENV === 'development') {

        socket.on('ePortalToDnbCurl', function (data) {
            logger.info(data);
            const dataObj = JSON.parse(data);
            const url = dataObj.url;
            const method = dataObj.method || 'GET';
            // make a get request to the url using axios with base url as current server in dev mode which is localhost:3000
            const axios = require('axios');
            const axiosInstance = axios.create({
                baseURL: currentLocalDevServer,
                timeout: 1000,
            });
            axiosInstance({
                method,
                url,
            }).then((response) => {
                logger.info(response.data);
                socket.emit("ePortalToDnbCurlResult", response.data);
            }).catch((error) => {
                logger.error(error);
            });

        });


    }
    return socket;


}

module.exports = {
    createSocket,
}
