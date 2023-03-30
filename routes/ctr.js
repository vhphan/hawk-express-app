const express = require("express");
const router = express.Router();
const path = require("path");
const { dirname } = require("path");
const AdmZip = require("adm-zip");
const fs = require("fs");
const multer = require('multer');

const upload = multer({ dest: 'uploads/' });
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

const runCommands = function (logFilePath, commands) {
  // run commands as a child process
  const { exec } = require("child_process");
  commands.forEach((command) => {
    exec(command, (error, stdout, stderr) => {

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
  });

}

router.post('/uploadShellScript', upload.single('file'), (req, res) => {

  // get uploaded file from form data
  const shellScriptFile = req.file;

  const shellScriptFileName = req.file.filename;
  const shellScriptFilePath = `${global.__basedir}/${req.file.path}`;
  const shellFolder = shellScriptFilePath.replace(shellScriptFileName, '');
  // save shell script file to folder
  // run shell script file
  const { exec } = require("child_process");
  // create a log file to store the output of the shell script
  const logFileName = shellScriptFileName.replace('.sh', '.log');

  // create an id for the log file
  const logFileId = new Date().getTime();
  const randomChars = Math.random().toString(36).slice(-4);
  const logFileNameWithId = randomChars + logFileId + logFileName;
  const logFilePath = `${shellFolder}${logFileNameWithId}`;
  // create log file
  fs.writeFileSync(logFilePath, '');

  const shellCommands = [
    'cd /home/hawkuser/RAN/CTR_Files_bot-sftp-sharepoint-service-ver1',
    'cd /tmp',
    'rm *_DNBCTR.tar.gz'
  ];


  // filter commands from shell script file
  // with the following regex expression
  // ^(find\s\/[\w\/\.]+\s-name\s"[^|&;()]+\.[a-zA-Z0-9]{2,4}"\s-type\s+f\s-print)$
  const readline = require('readline');
  const { createInterface } = readline;

  const regex = /^\s*find \/data4\/.*$/;
  const pattern = /\*\d{4}\+\d{4}\-\d{4}\+\d{4}\_\*\w+\*CUCP\*\.gpb\.gz/g;

  // read all lines in file into array lines
  const lines = fs.readFileSync(shellScriptFilePath, 'utf-8').split(/\r?\n/);


  const rootFolders = [];
  const filePatterns = [];
  const targetZipFiles = [];
  lines.forEach((line) => {
    // return regex.test(command);
    if (regex.test(line)) {
      const filePattern = line.match(pattern);
      const rootFolder = line.match(/data4\/([a-zA-Z]+)\/CTR_LOGS/);
      const zipFile = line.match(/[^/]*\.tar\.gz$/); // match the last word in the line
      // return if any of the two variables is null
      if (!filePattern || !rootFolder || !zipFile) {
        return;
      }

      filePatterns.push(filePattern);
      rootFolders.push(rootFolder);
      targetZipFiles.push(zipFile);

    }
  });

  const fileCommands = filePatterns.map((filePattern, index) => {
    const rootFolder = rootFolders[index];
    const zipFile = targetZipFiles[index];
// find /data4/BRF/CTR_LOGS/bot-sftp-sharepoint-service-ver1-CTR__EANKRAG__20230308??????/CTR_Files/????????/ -name "*0800+0800-0815+0800_*DBPET0915_TABUNGHAJI*CUCP*.gpb.gz" -type f -print | xargs tar -czf /tmp/0800+0800-0815+0800_DBPET0915_CUCP_DNBCTR.tar.gz

    return `find ${rootFolder}/bot-sftp-sharepoint-service-ver1-CTR__EANKRAG__20230308??????/CTR_Files/????????/ -name "${filePattern}" -type f -print | xargs tar -czf /tmp/${zipFile}`;
  });

  // add filtered commands to shell commands
  shellCommands.push(...fileCommands);

  const lastCommands = [

    'cd /home/hawkuser/RAN/CTR_Files_bot-sftp-sharepoint-service-ver1',
    'java -jar bot-sftp-sharepoint-service-ver1-0.0.1-SNAPSHOT.jar "bot-sftp-sharepoint-service-ver1-MANUALCTR_" --spring.config.location=./MANUAL_CTR_CONFIG/',
    'cd /tmp',
    'rm *_DNBCTR.tar.gz'

  ]

  shellCommands.push(...lastCommands);

  // run shell commands
  // runCommands(logFilePath, shellCommands);

  const downloadLink = "shellLog/" + logFileNameWithId;

  res.json({
    success: true,
    data: {
      shellScriptFilePath,
      logFile: logFileNameWithId,
      commands: shellCommands,
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

