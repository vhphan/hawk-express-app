const express = require("express");
const router = express.Router();
const path = require("path");
const { dirname } = require("path");
const AdmZip = require("adm-zip");
const fs = require("fs");

const appDir = global.__basedir;

const {
  searchFilesWithPatternAndMeContext,
  getMeContextFromString,
  zipListOfFiles } = require("./utils");
const { fstat } = require("fs");



router.get('/hello', (req, res) => {
  res.send('Hello From CTR!' + appDir);
});

router.get("/ctr-dl", function (req, res) {
  res.sendFile(path.join(appDir, "/assets/ctr-dl.html"));
});

router.get('/ctr-dl.js', function (req, res) {
  res.sendFile(path.join(appDir, "/assets/ctr-dl.js"));
});


router.get("/getAvailableSites", function (req, res) {
  const { selectedDate, selectedHour, siteSubstring } = req.query;
  console.log(selectedDate, selectedHour, siteSubstring);

  const files = searchFilesWithPatternAndMeContext('/data4', selectedDate.replaceAll('-', '') + '.' + selectedHour, siteSubstring);

  const meContextValues = files.map((file) => {
    return getMeContextFromString(file);
  });
  const uniqueMeContextValues = [...new Set(meContextValues)];

  res.json({
    success: true,
    data: uniqueMeContextValues,
  });

});


router.get('/bootstrap.bundle.min.js', (req, res) => {
  res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js')
});

router.get('/bootstrap.bundle.min.js.map', (req, res) => {
  res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js.map')
});

router.get('/bootstrap.min.css', (req, res) => res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/css/bootstrap.min.css'));

router.get('/bootstrap.min.css.map', (req, res) => res.sendFile(global.__basedir + '/node_modules/bootstrap/dist/css/bootstrap.min.css.map'));


router.get('/downloadSelectedSites', async (req, res) => {

  console.log(req.query);
  const { selectedDate, selectedHour, selectedHourEnd, sites, fileTypes } = req.query;

  // loop through all hours and get all files
  let finalListOfFiles = [];

  for (let i = parseInt(selectedHour); i <= parseInt(selectedHourEnd); i++) {
    const hour = i < 10 ? '0' + i : i;
    const dateHour = selectedDate.replaceAll('-', '') + '.' + hour;
    const initialListOfFiles = searchFilesWithPatternAndMeContext('/data4', dateHour, sites);

    // filter files by file type
    const filteredListOfFiles = initialListOfFiles.filter((file) => {
      const fileTypesChecks = fileTypes.map((fileType) => {
        if (file.includes('_celltracefile_' + fileType)) {
          return true;
        }
        return false;
      });
      return fileTypesChecks.includes(true);
    });

    finalListOfFiles = finalListOfFiles.concat(filteredListOfFiles);

  }

  const zipName = new Date().getTime();
  const randomChars = Math.random().toString(36).slice(-4);
  const zipFilename = randomChars + zipName + ".zip";
  const zipFilePath = global.__basedir + '/tmp/dl/' + zipFilename;

  // create a zip archive and add all files to it
  zipListOfFiles(finalListOfFiles, zipFilePath);

  // create a download link to the zip file
  const downloadLink = "tmp/dl/" + zipFilename;

  res.json({
    success: true,
    data: {
      // zipFilePath: zipFilePath
      downloadLink,
      zipFilename
    }
  });


});

router.get('/tmp/dl/:filename', (req, res) => {
  const { filename } = req.params;
  const filePath = global.__basedir + '/tmp/dl/' + filename;
  res.download(filePath);
});

// ======================================================================
// upload shell script file endpoint
// ======================================================================


router.get("/ctr-sh", function (req, res) {
  res.sendFile(path.join(appDir, `/assets/ctr-sh.html`));
});

router.get("/js/:jsFile", function (req, res) {
  const { jsFile } = req.params;
  res.sendFile(path.join(appDir, `/assets/${jsFile}`));
});



router.post('/uploadShellScript', (req, res) => {

  console.log(req.files);
  const { shellScriptFile } = req.files[0];
  const shellScriptFileName = shellScriptFile.name;
  const shellFolder = '/home/hawkuser/RAN/CTR_Files_bot-sftp-sharepoint-service-ver1';
  const shellScriptFilePath = `${shellFolder}/${shellScriptFileName}`;
  // save shell script file to folder
  fs.writeFileSync(shellScriptFilePath, shellScriptFile.data);
  // run shell script file
  const { exec } = require("child_process");
  // create a log file to store the output of the shell script
  const logFileName = shellScriptFileName.replace('.sh', '.log');

  // create an id for the log file
  const logFileId = new Date().getTime();
  const randomChars = Math.random().toString(36).slice(-4);
  const logFileNameWithId = randomChars + logFileId + logFileName;

  const logFilePath = `${shellFolder}/${logFileNameWithId}`;
  // create log file
  fs.writeFileSync(logFilePath, '');
  // run shell script

  exec(`sh ${shellScriptFilePath}`, (error, stdout, stderr) => {

    if (error) {
      console.log(`error: ${error.message}`);
      // write error to log file
      fs.appendFileSync(logFilePath, error.message);
      return;
    }
    if (stderr) {
      console.log(`stderr: ${stderr}`);
      fs.appendFileSync(logFilePath, stderr);
      return;
    }
    console.log(`stdout: ${stdout}`);
    fs.appendFileSync(logFilePath, stdout);

  });

  const downloadLink = "shellLog/" + logFileNameWithId;

  res.json({
    success: true,
    data: {
      shellScriptFilePath,
      logFile: logFileNameWithId,
      downloadLink
    }
  });


});


router.get('/shellLog/:logFile', (req, res) => {

  const { logFile } = req.params;
  const shellFolder = '/home/hawkuser/RAN/CTR_Files_bot-sftp-sharepoint-service-ver1';
  const filePath = shellFolder + '/' + logFile;
  res.download(filePath);

});

module.exports = router;

