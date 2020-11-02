const cron = require('node-cron')
const fs = require('fs')
const got = require('got')
const darkSkyToken = process.env['TOKEN_DARKSKY']
const { sqlQuery } = require('../utils/postgres')

/* Utility functions */
Date.prototype.addDays = function(days) {
  const d = new Date(this.valueOf())
  d.setDate(d.getDate() + days)
  return d
}

async function sleepForMs(ms) { return new Promise(r => setTimeout(r, ms)) }
function dnow() { return new Date().toISOString() }

/* Uncomment the line below for testing */
// init().then(() => { process.exit() })
runonce().then(() => { console.log(`[${dnow()}] : INIT COMPLETE`)})

async function runonce() {
  await prepareDB()
}

/*  Main function */
async function init() {
  const cityList = JSON.parse(fs.readFileSync('./config/cityList.json', 'utf-8'))
  await processCities(cityList)
}

async function insertDateInfo(sqlText) {
  let queryText = `INSERT INTO "public".darksky_dailyweather` +
    `(cityname, eventtime, rawdata)	VALUES ${sqlText.slice(0, -1)} ` +
    `ON CONFLICT DO NOTHING`
  await sqlQuery(queryText)
}

async function processCities(cityList) {
  const dateArray = []
  const maxCount = 4
  const url = 'https://api.darksky.net/forecast'

  const data = await sqlQuery({
    text: `SELECT * FROM "public".darksky_dailyweather WHERE rawdata = $1 ` +
        `ORDER BY eventtime DESC LIMIT ${Math.floor(maxCount * 2)}`,
    values: [ '{}']
  })

  data.dbResult.forEach((d) => {
    const dt = {
      city: d.cityname,
      date: new Date(`${d.eventtime.slice(0,10)}T06:00:00.000Z`)
    }
    dateArray.push(dt)
  })

  try {
    let c = 0
    for (let i = 0; i < dateArray.length; ++i) {
      const city = dateArray[i].city
      const lat = cityList[city].lat
      const lon = cityList[city].lon

      const dateToProcess = dateArray[i].date.getTime() / 1000
      const dataResp = await got(`${url}/${darkSkyToken}/${lat},${lon},${dateToProcess}`)
      const weatherData = JSON.parse(dataResp.body)
      const insertText = `UPDATE "public".darksky_dailyweather SET rawdata = '${JSON.stringify(weatherData)}' ` +
        `WHERE cityname = '${city}' AND eventtime = '${new Date(dateToProcess * 1000).toISOString()}'`

      await sqlQuery(insertText)
      await sleepForMs(1000)

      console.log(`[${dnow()}]: Processed [${city}] for date ${new Date(dateToProcess * 1000)}`)
      if (++c >= maxCount) break
    }
  } catch (excp) {
    console.log(`[EXCEPTION]: ${excp.message}`)
  } finally {
    // Do nothing
    console.log(`[${dnow()}]: Finished processing cities`)
  }
}

async function prepareDB() {
  const cityList = JSON.parse(fs.readFileSync('./config/cityList.json', 'utf-8'))
  const dateRange = ['2019-01-01', '2020-10-31']
  const dateArray = []

  let firstDate = new Date(`${dateRange[0]}T06:00:00.000Z`)
  const lastDate = new Date(`${dateRange[1]}T06:00:00.000Z`)
  while (firstDate <= lastDate) {
    const dateToInsert = new Date(`${firstDate.toISOString().slice(0,10)}T06:00:00.000Z`)
    dateArray.push(dateToInsert)
    firstDate = firstDate.addDays(1)
  }

  let ctr = 0
  for (const [k, v] of Object.entries(cityList)) {
    let insertText = ''
    for (let i = 0; i < dateArray.length; ++i) {
      const dateToProcess = dateArray[i].getTime() / 1000
      insertText += `('${k}', '${new Date(dateToProcess * 1000).toISOString()}', '{}'),`
      if (++ctr % 1000 === 0) {
        await insertDateInfo(insertText)
        insertText = ''
        ctr = 0
      }
    }

    if (insertText !== '') { await insertDateInfo(insertText) }
  }
}

cron.schedule('0 */15 * * * *', () => {
  console.log(`[${dnow()}]: Starting DarkSky Fetch`)
  init().then(() => { console.log(`[${dnow()}]: DONE`); return {} })
})
