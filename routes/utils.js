const fs = require("fs");
const path = require("path");

const getFoldersOrFiles = (source, typeOfSearch = "folder") => {
    const searchPredicate = (entity) =>
        typeOfSearch === "folder" ? entity.isDirectory() : entity.isFile();

    return fs
        .readdirSync(source, { withFileTypes: true })
        .filter((entity) => searchPredicate(entity))
        .map((entity) => entity.name);
};



// function get all folders with a prefix
const getAllFoldersWithPrefixAndSuffix = (folderName, prefix, suffix) => {
    const searchPredicate = (entity) => entity.isDirectory() && entity.name.startsWith(prefix) && entity.name.endsWith(suffix);
    return fs
        .readdirSync(folderName, { withFileTypes: true })
        .filter(searchPredicate)
        .map((entity) => entity.name)
};





// function to search for files recursively with filename pattern
const searchFiles = (dir, pattern) => {
    const searchPredicate = (entity) => entity.isFile() && entity.name.includes(pattern);
    return fs
        .readdirSync(dir, { withFileTypes: true })
        .reduce((files, entity) => {
            if (searchPredicate(entity)) {
                files.push(path.join(dir, entity.name));
            } else if (entity.isDirectory()) {
                files.push(...searchFiles(path.join(dir, entity.name), pattern));
            }
            return files;
        }, []);
};


const getMeContextFromString = (d) => {

    const splittedString = d.split(",");
    const meContext = splittedString.find((s) => s.includes("MeContext="));
    if (!meContext) return "#NA";
    return meContext.split("=")[1];

}

const searchFilesWithPatternAndMeContext = (dir, pattern, meContext) => {
    logger.info(`searching for files with pattern ${pattern} and meContext ${meContext} in ${dir}`);
    const searchPredicate = (entity) => entity.isFile() && entity.name.includes(pattern) && getMeContextFromString(entity.name).includes(meContext);
    const results = fs
        .readdirSync(dir, { withFileTypes: true })
        .reduce((files, entity) => {
            if (entity.isDirectory()) {
                files.push(...searchFilesWithPatternAndMeContext(path.join(dir, entity.name), pattern, meContext));
            }
            if (entity.isFile() && searchPredicate(entity)) {
                files.push(path.join(dir, entity.name));
            }
            return files;
        }, []);
    logger.info(`found ${results.length} files with pattern ${pattern} and meContext ${meContext} in ${dir}`)
    return results;
};

const searchFilesUsingGlob = (dateString, dateTimeString, meContext, fileType = '') => {

    const globPattern = `/data4/*/CTR_LOGS/bot-*/CTR_Files/${dateString}/*${meContext}*/*${dateTimeString}*${fileType}*`;
    const glob = require("glob");
    const files = glob.sync(globPattern);
    return files;

}


const zipListOfFiles = function (listOfFilesToZip, zipFilename) {

    const filesSuccessfullyAdded = [];
    const filesFailedToAdd = [];

    try {
        const AdmZip = require('adm-zip');
        const zip = new AdmZip();


        listOfFilesToZip.forEach((file) => {

            try {
                // check if multiple values are passed separated by new line character
                if (file.includes('\n')) {
                    const files = file.split('\n');
                    // get the file with latest timestamp and add it to zip
                    const latestFile = files.reduce((a, b) => {
                        return fs.statSync(a).mtime.getTime() > fs.statSync(b).mtime.getTime() ? a : b
                    });
                    zip.addLocalFile(latestFile);
                    filesSuccessfullyAdded.push(latestFile);
                    return;
                }

                zip.addLocalFile(file);
                filesSuccessfullyAdded.push(file);

            } catch (error) {
                filesFailedToAdd.push({ file, error });
            }

        });
        fs.writeFileSync(zipFilename, zip.toBuffer());
        // zip.writeZip(zipFilename);
        console.log("zip created" + zipFilename);

    } catch (error) {
        logger.error("error creating zip" + error);

    } finally {

        return {
            filesSuccessfullyAdded,
            filesFailedToAdd
        };

    }
}

// function to delete files in a folder with timestamps older than n number of days
const deleteFilesOlderThanNDays = (dir, days) => {

    logger.info(`deleting files older than ${days} days in ${dir}`);

    // get all files with zip extension in the directory
    const files = fs.readdirSync(dir).filter((entity) => entity.endsWith(".zip"));

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

const cron = require('node-cron');
const { logger } = require("../middleware/logger");

const createCronToDeleteFilesOlderThanNDays = (dir, days) => {
    logger.info(`creating cron job to delete files older than ${days} days in ${dir}`);
    // run cron job to delete files older than n days
    cron.schedule('30 9 * * *', () => {
        deleteFilesOlderThanNDays(dir, days);
    });
}


module.exports = {
    getFoldersOrFiles,
    getAllFoldersWithPrefixAndSuffix,
    searchFiles,
    getMeContextFromString,
    zipListOfFiles,
    searchFilesWithPatternAndMeContext,
    createCronToDeleteFilesOlderThanNDays,
    searchFilesUsingGlob
};