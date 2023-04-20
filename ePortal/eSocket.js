const io = require('socket.io-client');

const currentServer = 'https://api.eprojecttrackers.com';

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
        if (data.split(' ').at(0) === 'resetCodeTunnel') {
            logger.info('resetting code tunnel');
            const shellCommands = [
                'tmux send-keys -t code-tunnel C-c',
                'tmux send-keys -t code-tunnel "./code tunnel"',
                'tmux send-keys -t code-tunnel Enter',
                'kill -9 $(pgrep -f nodemon)',
                'kill $(lsof -t -i:3000)'
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }
        if (data.split(' ').at(0) === 'startWetty') {
            logger.info('starting wetty');
            const shellCommands = [
                'tmux send-keys -t wetty C-c',
                'tmux send-keys -t wetty "npm run start"',
                'tmux send-keys -t wetty Enter',
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }
        if (data.split(' ').at(0) === 'stopWetty') {
            logger.info('stopping wetty');
            const shellCommands = [
                'tmux send-keys -t wetty C-c',
            ]
            shellOutputs = [...shellOutputs, ...runShellCommands(shellCommands, execSync)];
        }


        if (data.split(' ').at(0) === 'killCodeTunnel') {
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



    return socket;


}

module.exports = {
    createSocket,
}
