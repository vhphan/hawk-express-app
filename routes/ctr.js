const express = require("express");
const router = express.Router();
const fs = require("fs");
const path = require("path");
const { dirname } = require("path");
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
  const { timeStamp, siteSubString } = req.query;
  const sites = getFoldersInDaySubFolder(timeStamp).filter((d) => {
    const splittedString = d.split(",");
    const meContext = splittedString.find((s) => s.includes("MeContext="));
    const meContextValue = meContext.split("=")[1];
    return meContextValue.includes(siteSubString);
  });

  res.json({
    success: true,
    data: sites,
  });
});

module.exports = router;
