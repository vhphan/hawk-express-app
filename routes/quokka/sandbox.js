const { logger } = require("../../middleware/logger");
const fs = require('fs');
const path = require('path');

const deleteFilesOlderThanNDays = (dir, days) => {

    logger.info(`deleting files older than ${days} days in ${dir}`);

    // get all files with zip extension in the directory
    const files = fs.readdirSync(dir).filter((entity)=> entity.endsWith(".zip"));

    // get the current time
    const now = new Date().getTime();

    // get files in folder older than n days
    const filesToDelete = files.filter((file) => {
        const fileStats = fs.statSync(path.join(dir, file));
        const fileCreationTime = fileStats.ctime.getTime();
        const diff = now - fileCreationTime;
        return diff > days * 24 * 60 * 60 * 1000;
    });

    // delete files
    filesToDelete.forEach((file) => {
        const filePath = path.join(dir, file);
        // delete file
        logger.info(`deleting file ${filePath}`);
        fs.unlinkSync(filePath);
    });

}

deleteFilesOlderThanNDays('/data2/var/www/hawk-express-app/tmp/dl', 2);