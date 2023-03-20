const express = require("express");
const router = express.Router();
const fs = require("fs");
const path = require("path");
const { dirname } = require("path");
const AdmZip = require("adm-zip");
const asyncHandler = require("../middleware/async");

const appDir = dirname(require.main.filename);

const getFoldersOrFiles = (source, typeOfSearch = "folder") => {
  const searchPredicate = (entity) =>
    typeOfSearch === "folder" ? entity.isDirectory() : entity.isFile();

  return fs
    .readdirSync(source, { withFileTypes: true })
    .filter((entity) => searchPredicate(entity))
    .map((entity) => entity.name);
};

// /data4/BRF/CTR_LOGS/
const getCtrLogsFolders = () => {
  const ctrLogsPath = "/data4/BRF/CTR_LOGS/";
  const ctrLogsFolders = getFoldersOrFiles(ctrLogsPath);
  const timeStamps = ctrLogsFolders.map((folder) => {
    const timeStamp = folder.split("_").at(-1);
    return timeStamp;
  });
  return timeStamps;
};

const getFoldersInDaySubFolder = (timeStamp) => {
  const ctrLogsPath = "/data4/BRF/CTR_LOGS/";
  const folderPrefix = "bot-sftp-sharepoint-service-ver1-BRF-CTR__EANKRAG__";
  const folderPath =
    ctrLogsPath +
    folderPrefix +
    timeStamp +
    "/CTR_Files/" +
    timeStamp.slice(0, 8) +
    "/";
  const files = getFoldersOrFiles(folderPath, "folder");
  return files;
};


const getMeContextFromString = (d) => {

  const splittedString = d.split(",");
  const meContext = splittedString.find((s) => s.includes("MeContext="));
  return meContext.split("=")[1];

}

const zipListOfFiles = function(listOfFilesToZip, zipFilename){
  const AdmZip = require('adm-zip');
  const zip = new AdmZip();
  listOfFilesToZip.forEach((file) => {
    zip.addLocalFile(file);
  });
  fs.writeFileSync(zipFilename, zip.toBuffer());
  console.log("zip created" + zipFilename);
}



router.get("/ctr-dl", function (req, res) {
  res.sendFile(path.join(appDir, "/assets/ctr-dl.html"));
});

router.get("/getAvailableDatas", function (req, res) {
  const timeStamps = getCtrLogsFolders();
  const files = timeStamps.map((timeStamp) => {
    return getFoldersInDaySubFolder(timeStamp);
  });

  // create object with timeStamps as keys and files as values
  const data = timeStamps.reduce((acc, timeStamp, index) => {
    acc[timeStamp] = files[index];
    return acc;
  }, {});

  res.json({
    success: true,
    data: data,
  });
});

router.get("/getAvailableTimestamps", function (req, res) {
  const timeStamps = getCtrLogsFolders();
  res.json({
    success: true,
    data: timeStamps,
  });
});

router.get("/getAvailableSites", function (req, res) {
  const { timestamp, siteSubstring } = req.query;
  const sites = getFoldersInDaySubFolder(timestamp);
  // const filteredSites = sites.filter((d) => {
  //   const splittedString = d.split(",");
  //   const meContext = splittedString.find((s) => s.includes("MeContext="));
  //   const meContextValue = meContext.split("=")[1];
  //   return meContextValue.includes(siteSubstring);
  // });
  const meContextValues = sites.map(getMeContextFromString);
  const uniqueMeContextValues = [...new Set(meContextValues)];
  const filterdMeContextValues = uniqueMeContextValues.filter((d) =>
    d.toLowerCase().includes(siteSubstring.toLowerCase())
  );

  res.json({
    success: true,
    data: filterdMeContextValues,
  });
});



router.get('/bootstrap.bundle.min.js', (req, res) => {
  res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js')
});

router.get('/bootstrap.min.css', (req, res) => res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/css/bootstrap.min.css'));

// /data4/BRF/CTR_LOGS/bot-sftp-sharepoint-service-ver1-BRF-CTR__EANKRAG__20230315165518/CTR_Files/20230315
router.get('/downloadSelectedSites', async (req, res) => {

  console.log(req.query);
  const { timestamp, sites } = req.query;
  const ctrLogsPath = "/data4/BRF/CTR_LOGS/";

  // get all folders in ctrLogsPath
  const ctrLogsFolders = getFoldersOrFiles(ctrLogsPath);
  const foldersWithTimestamp = ctrLogsFolders.filter((folder) => {
    return folder.endsWith(timestamp);
  });
  let finalListOfFiles = [];
  foldersWithTimestamp.forEach((folder) => {
    const folderPath = ctrLogsPath + folder + "/CTR_Files/" + timestamp.slice(0, 8) + "/";
    const sitesFolders = getFoldersOrFiles(folderPath, "folder").filter((d) => {
      const meContextValue = getMeContextFromString(d);
      return sites.includes(meContextValue);
    });

    // array.reduce(function(total, currentValue, currentIndex, arr), initialValue)
    // const folder = folderPath + d;
    // const files = getFoldersOrFiles(folder, "file");

    const finalFiles = sitesFolders.reduce((acc, d) => {
      const folder = folderPath + d;
      const files = getFoldersOrFiles(folder, "file");
      const filesWithFullPath = files.map((file) => {
        return folder + "/" + file;
      });
      acc = [...acc, ...filesWithFullPath];
      return acc;
    }, []);

    finalListOfFiles = [...finalListOfFiles, ...finalFiles];

  });

  const zipName = new Date().getTime();
  const randomChars = Math.random().toString(36).slice(-4);
  const zipFilePath = global.__basedir + '/tmp/dl/' + randomChars + zipName + ".zip";

  // create a zip archive and add all files to it
  zipListOfFiles(finalListOfFiles, zipFilePath);


  res.json({
    success: true,
    data: {zipFilePath: zipFilePath}
  });

});



module.exports = router;
