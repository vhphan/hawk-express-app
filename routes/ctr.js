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
  zipListOfFiles,
  searchFilesUsingGlob,
} = require("./utils");
const { fstat } = require("fs");
const { logger } = require("../middleware/logger");
// const { sendEmail } = require("../utils");



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

  logger.info(`getAvailableSites: ${selectedDate}, ${selectedHour}, ${siteSubstring}`);

  // const files = searchFilesWithPatternAndMeContext('/data4', selectedDate.replaceAll('-', '') + '.' + selectedHour, siteSubstring);

  const files = searchFilesUsingGlob(selectedDate.replaceAll('-', ''), selectedDate.replaceAll('-', '') + '.' + selectedHour, siteSubstring);


  logger.info(`found ${files.length} files for ${selectedDate}, ${selectedHour}, ${siteSubstring}`)

  const meContextValues = files.map((file) => {
    return getMeContextFromString(file);
  });
  const uniqueMeContextValues = [...new Set(meContextValues)];

  logger.info(`found ${uniqueMeContextValues.length} sites for ${selectedDate}, ${selectedHour}, ${siteSubstring}`)

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

  try {
    logger.info('downloadSelectedSites');
    logger.info(req.query);
    const { selectedDate, selectedHour, selectedHourEnd, sites, fileTypes } = req.query;

    // loop through all hours and get all files
    let finalListOfFiles = [];

    for (let i = parseInt(selectedHour); i <= parseInt(selectedHourEnd); i++) {
      const hour = i < 10 ? '0' + i : i;
      const dateHour = selectedDate.replaceAll('-', '') + '.' + hour;
      // const initialListOfFiles = searchFilesWithPatternAndMeContext('/data4', dateHour, sites);

      const initialListOfFiles = sites.reduce((acc, site) => {
        const files = searchFilesUsingGlob(selectedDate.replaceAll('-', ''), dateHour, site);
        return acc.concat(files);
      }, []);

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

  } catch (error) {

    logger.error(error);
    res.json({
      success: false,
      message: error.message || 'Error while downloading selected sites'
    });

  }


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

router.get("/ctr-dl-v2", function (req, res) {
  res.sendFile(path.join(appDir, `/assets/ctr-dl-v2.html`));
});

router.get("/js/:jsFile", function (req, res) {
  const { jsFile } = req.params;
  res.sendFile(path.join(appDir, `/assets/${jsFile}`));
});

// set assets folder as static folder for this route
router.use('/assets', express.static(path.join(appDir, '/assets')));



const runCommands = function (logFilePath, commands) {
  // run commands as a child process
  const { execSync } = require("child_process");
  const shellSuccessfullOutputs = [];
  const shellErrorOutputs = [];
  commands.forEach((command) => {
    try {
      let out = execSync(command);
      //ONLY replace new line character if it is at the end of the line
      out = out.toString().replace(/\n$/, "");
      shellSuccessfullOutputs.push(out.toString());
    } catch (error) {
      shellErrorOutputs.push(error.message);
    }
  });
  return {
    shellSuccessfullOutputs,
    shellErrorOutputs,
  };
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


router.post('/uploadCsvFile', upload.single('file'), async (req, res) => {

  const csvFileName = req.file.filename;
  const csvFilePath = `${global.__basedir}/${req.file.path}`;

  // read the sample csv file into a JSON array
  const csv = require('csvtojson');
  const sampleCsvFilePath = `${global.__basedir}/assets/sample-ctr-dl-v2.csv`;
  const sampleFileJson = await csv().fromFile(sampleCsvFilePath);

  // read the csv file into a JSON array
  const csvFileJson = await csv().fromFile(csvFilePath);

  // compare the two JSON keys
  const sampleKeys = Object.keys(sampleFileJson[0]);
  const csvKeys = Object.keys(csvFileJson[0]);
  // check if keys are equal and disregard the order
  const isEqual = sampleKeys.length === csvKeys.length && sampleKeys.sort().every((value, index) => value === csvKeys.sort()[index]);

  if (!isEqual) {
    return res.json({
      success: false,
      message: 'Invalid CSV file format. Please download and use the sample CSV file as reference.'
    });
  }

  // use glob to find file
  // const rootFolders = ['/data4/BRF/', '/data4/KVDC/'];
  const glob = require('glob');
  const shellCommands = [];
  const filesResults = csvFileJson.map((row) => {
    const siteName = row['Site name'];
    const dateString = row['Date'];
    const timeString = row['Time'];
    const fileType = row['Type'];

    const siteId = siteName.split('_')[0];

    const globPattern = `/data4/*/CTR_LOGS/bot-*/CTR_Files/${dateString}/*${siteId}*/*${dateString}*${timeString}*${fileType}*`;
    // construct shell command to find file
    const shellCommand = `find ${globPattern} -type f -print`;
    shellCommands.push(shellCommand);
    logger.info(shellCommand);


  });

  logger.info(filesResults);
  // create log file with timestamp
  const logFileName = 'shell-log-' + Date.now() + '.txt';
  const logFilePath = `${global.__basedir}/logs/${logFileName}`;
  const { shellSuccessfullOutputs, shellErrorOutputs } = runCommands(logFilePath, shellCommands);



  // create zip file with timestamp and random prefix of 5 characters
  const zipFileName = 'CTR-' + Date.now() + '-' + Math.random().toString(36).substring(2, 7) + '.zip';
  const zipFilePath = `${global.__basedir}/tmp/dl/${zipFileName}`;

  // create download link
  const downloadLink = "tmp/dl/" + zipFileName;

  const {
    filesSuccessfullyAdded,
    filesFailedToAdd
  } = zipListOfFiles(shellSuccessfullOutputs, zipFilePath);

  const filesFailedToFind = shellErrorOutputs.map(
    (errorOutput) => {
      if (errorOutput.includes('Command failed: find ')) {
        return errorOutput.replace('Command failed: find ', '').split(' -type f -print')[0];
      }
      return errorOutput;
    }
  );

  let csvString = 'File name,Status,Error message\n';
  filesSuccessfullyAdded.forEach(f => csvString += `"${f.split('/').at(-1)}",Success,\n`);
  filesFailedToAdd.forEach(f => csvString += `"${f.file.split('/').at(-1)}",Failed, "${f.error}"\n`);
  filesFailedToFind.forEach(f => csvString += `"${f}",Failed, File not found\n`);

  // create a csv file with the results
  const csvFileNameWithId = 'results-' + Date.now() + '-' + Math.random().toString(36).substring(2, 7) + '.csv';
  const csvFilePathWithId = `${global.__basedir}/tmp/dl/${csvFileNameWithId}`;
  // write to csv file using appendFileSync
  fs.appendFileSync(csvFilePathWithId, csvString);

  const csvFileDownloadLink = "tmp/dl/" + csvFileNameWithId;

  // upload zip file to sharepoint
  // uploadFileToSharepoint(zipFilePath);

  try{
    
    uploadFileToSharepointUsingPython(zipFileName);
  } catch (err){
    logger.error(err);
  }


  res.json({
    success: true,
    data: {
      shellSuccessfullOutputs,
      shellErrorOutputs,
      numberOfFilesNotFound: filesFailedToFind.length,
      numberOfFilesZipped: filesSuccessfullyAdded.length,
      numberOfFilesFailedToZip: filesFailedToAdd.length,
      logFile: logFileName,
      downloadLink,
      csvFileDownloadLink,
      zipFileName,
    },
  });

});


function uploadFileToSharepointUsingPython(fileName) {

  logger.info('uploadFileToSharepointUsingPython started')

  const { spawn } = require('child_process');
  const pythonScriptPath = '/data2/var/www/hawk-express-app/pyscript/upload_sp.py';
  const pythonInterpreter = '/home/hawkuser/miniconda3/envs/hawk-ctr/bin/python';
  const pythonProcess = spawn(pythonInterpreter, [pythonScriptPath, fileName]);

  pythonProcess.stdout.on('data', (data) => {
    logger.info(`stdout: ${data}`);
  });

  pythonProcess.stderr.on('data', (data) => {
    logger.info(`stderr: ${data}`);
  });

  pythonProcess.on('close', (code) => {
    logger.info(`child process exited with code ${code}`);
  });

  // on error
  pythonProcess.on('error', (err) => {
    logger.error('Eror on Python script.');
    logger.error(err);
  });

}


function uploadFileToSharepoint(filePath) {

  const { spsave } = require("spsave");
  const spClientId = process.env.SP_CLIENT_ID;
  const spClientSecret = process.env.SP_CLIENT_SECRET;
  const spSiteUrl = process.env.SP_SITE_URL;
  const spFolder = process.env.SP_FOLDER;
  const spFileName = filePath.split('/').at(-1);

  const coreOptions = {
    siteUrl: spSiteUrl,
  };

  const creds = {
    clientId: spClientId,
    clientSecret: spClientSecret,
  };

  const fileOptions = {
    folder: spFolder,
    fileName: spFileName,
    fileContent: fs.readFileSync(filePath)
  };

  // sendEmail('vee.huen.phan@ericsson.com', 'File uploaded to SharePoint', 'Uploading file to SharePoint starts: ' + new Date().toISOString());

  spsave(coreOptions, creds, fileOptions).then(() => {
    logger.info('File uploaded to SharePoint');
    const msg = `File: ${fileOptions.fileName} uploaded to SharePoint`;
    // sendEmail('vee.huen.phan@ericsson.com', 'File uploaded to SharePoint', msg);
  }).catch((error) => {
    logger.error(error);
  });

}


module.exports = router;

