const apiCache = require('apicache');
const {logger} = require("./logger");


const onlyStatus200 = (req, res) => res.statusCode === 200;

const cache = apiCache.middleware;
const cacheLongTerm = cache('10 days', onlyStatus200);
const cache15m = cache('15 minutes', onlyStatus200);
const cache30m = cache('30 minutes', onlyStatus200);
const cache2h = cache('2 hours', onlyStatus200);
const cache6h = cache('6 hours', onlyStatus200);
const cache12h = cache('12 hours', onlyStatus200);
const cache24h = cache('24 hours', onlyStatus200);

module.exports = {
    cache,
    cacheLongTerm,
    cache15m,
    cache30m,
    cache2h,
    cache12h,
    cache24h,
    cache6h,
};