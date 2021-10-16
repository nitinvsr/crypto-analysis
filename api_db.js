//Download 1 min candles and aggregate them into higher timeframes.
require('dotenv').config();

const Influx = require('influx');
const _ = require('lodash');
const got = require('got');
const opn = require('open')
const client = new Influx.InfluxDB({
  database: 'test_db_1',
  port: 8086,
});

function epoch (date) {
    return Date.parse(date)
  }
  
const dateToday = new Date() 

const endTS = epoch(dateToday);
console.log(dateToday);

dateToday.setMonth(dateToday.getMonth() - 1);
const startTS = epoch(dateToday); 

// console.log(monthBefore);
console.log(dateToday);
console.log(startTS);

//returns all list of APIs to process
const getReqArray = ({ symbol, fromTS, toTS, timeframe }) => {
  const tfw = {
    '1m': 1 * 60 * 1000,
  };
  const barw = tfw[timeframe];
  const n = Math.ceil((toTS - fromTS) / (1000 * barw));
  return _.times(n, (i) => {
    const startTS = fromTS + i * 1000 * barw;
    // opn(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${timeframe}&startTime=${startTS}&limit=1000`);
    return `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${timeframe}&startTime=${startTS}&limit=1000`;
  });
};

//Load Data
loadData = async ({ symbol }) => {
  try {
    const reqArray = getReqArray({
      symbol,
      fromTS: startTS,
      toTS: endTS,
      timeframe: '1m',
    });
    const reqRespA = await Promise.all(reqArray.map((url) => got(url)));
    const reqDatA = reqRespA.map((r) => JSON.parse(r.body)).flat();
    const rows = reqDatA.map((o) => {
      return {
        measurement: 'btc_data',
        tags: {
          exchange: 'BINANCE',
          subtype: 'SPOT',
          symbol,
        },
        fields: {
          open: parseFloat(o[1]),
          high: parseFloat(o[2]),
          low: parseFloat(o[3]),
          close: parseFloat(o[4]),
          volume: parseFloat(o[5]),
        },
        timestamp: new Date(o[0]),
      };
    });

    //Writing into DB
    // const rowChunks = _.chunk(rows, 2000); //best to write in chunks of 2k records
    // await rowChunks.reduce(async (previousPromise, nextID) => {
    //   await previousPromise;
    //   return client.writePoints(nextID);
    // }, Promise.resolve());
    // console.log('Data stored successfully!');
  } 
  
  catch (err) {
    console.log(`Error while processing ${err}`);
  }
};

loadData({ symbol: 'BTCUSDT' });