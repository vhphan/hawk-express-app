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
    return meContext.split("=")[1];

}

const searchFilesWithPatternAndMeContext = (dir, pattern, meContext) => {
    const searchPredicate = (entity) => entity.isFile() && entity.name.includes(pattern) && getMeContextFromString(entity.name).includes(meContext);
    return fs
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
};


const zipListOfFiles = function (listOfFilesToZip, zipFilename) {

    const filesSuccessfullyAdded = [];
    const filesFailedToAdd = [];

    try {
        const AdmZip = require('adm-zip');
        const zip = new AdmZip();


        listOfFilesToZip.forEach((file) => {

            try {
                zip.addLocalFile(file);
                filesSuccessfullyAdded.push(file);

            } catch (error) {
                filesFailedToAdd.push({file, error});
            }

        });
        fs.writeFileSync(zipFilename, zip.toBuffer());
        // zip.writeZip(zipFilename);
        console.log("zip created" + zipFilename);

    } catch (error) {
        console.log("error creating zip" + error);

    } finally {

        return {
            filesSuccessfullyAdded,
            filesFailedToAdd
        };

    }
}


module.exports = {
    getFoldersOrFiles,
    getAllFoldersWithPrefixAndSuffix,
    searchFiles,
    getMeContextFromString,
    zipListOfFiles,
    searchFilesWithPatternAndMeContext
};