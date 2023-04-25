const express = require("express");
const router = express.Router();

const { testQuery } = require("../db/dailyKpiQueries");

router.get("/test", testQuery);

module.exports = router;
