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
//init().then(() => { process.exit() })
runonce().then(() => { console.log(`[${dnow()}] : INIT COMPLETE`)})

async function runonce() {
  const cityList = JSON.parse(fs.readFileSync('./config/cityList.json', 'utf-8'))
  const dateRange = ['1975-01-01', '2022-01-01']
  await prepareDB(cityList, dateRange)
}

/*  Main function */
async function init(shouldSort) {
  const cityList = JSON.parse(fs.readFileSync('./config/cityList.json', 'utf-8'))
  await processCities(cityList, shouldSort)
}

async function insertDateInfo(sqlText) {
  let queryText = `INSERT INTO "public".darksky_dailyweather` +
    `(cityname, eventtime, rawdata)	VALUES ${sqlText.slice(0, -1)} ` +
    `ON CONFLICT DO NOTHING`
  await sqlQuery(queryText)
}

async function processCities(cityList, shouldSort) {
  const dateArray = []
  const maxCount = 10
  const url = 'https://api.darksky.net/forecast'

  if (shouldSort === false) {
    const data = await sqlQuery({
      text: `SELECT * FROM "public".darksky_dailyweather WHERE rawdata = $1 ` +
          `ORDER BY RANDOM() LIMIT ${Math.floor(maxCount * 2)}`,
      values: [ '{}']
    })

    data.dbResult.forEach((d) => {
      const dt = {
        city: d.cityname,
        date: new Date(`${d.eventtime.slice(0,10)}T06:00:00.000Z`)
      }
      dateArray.push(dt)
    })
  }

  if (shouldSort === true) {
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
  }

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

async function prepareDB(cityList, dateRange) {
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

async function updateDate() {
  const dateYest = new Date()
  dateYest.setDate(dateYest.getDate() - 1)
  const fetchMaxDate = `SELECT cityname, MAX(DATE(eventtime)) FROM "public".darksky_dailyweather` +
    ` GROUP BY cityname`
  const someData = await sqlQuery(fetchMaxDate)
  if (someData.returnCode === 0 && someData.dbResult.length !== 0) {
    const dateArray = []
    
    someData.dbResult.forEach((r) => {
      let firstDate = new Date(`${r.max.slice(0,10)}T06:00:00.000Z`)
      firstDate = firstDate.addDays(1)
      const lastDate = new Date(`${dateYest.toISOString().slice(0,10)}T06:00:00.000Z`)

      while (firstDate <= lastDate) {
        const dateToInsert = new Date(`${firstDate.toISOString().slice(0,10)}T06:00:00.000Z`)
        dateArray.push({ 'c': r.cityname, 'd': dateToInsert})
        firstDate = firstDate.addDays(1)
      }
    })

    let ctr = 0
    let insertText = ''
    for (let i = 0; i < dateArray.length; ++i) {
      const dateToProcess = dateArray[i].d.getTime() / 1000
      insertText += `('${dateArray[i].c}', '${new Date(dateToProcess * 1000).toISOString()}', '{}'),`
      if (++ctr % 1000 === 0) {
        await insertDateInfo(insertText)
        insertText = ''
        ctr = 0
      }
    }

    if (insertText !== '') { await insertDateInfo(insertText) }
  }

}

cron.schedule('0 */20 * * * *', () => {
  console.log(`[${dnow()}]: Starting DarkSky Fetch`)
  init(false).then(() => { console.log(`[${dnow()}]: DONE`); return {} })
})

cron.schedule('0 11 * * * *', () => {
  console.log(`[${dnow()}]: Starting DarkSky Fetch`)
  init(true).then(() => { console.log(`[${dnow()}]: DONE`); return {} })
})

cron.schedule('0 5 */6 * * *', () => {
  console.log(`[${dnow()}]: Starting DarkSky Fetch`)
  init(true).then(() => { console.log(`[${dnow()}]: DONE`); return {} })
})

cron.schedule('0 28 0 * * *', () => {
  console.log(`[${dnow()}]: [UPDATE] Date for existing cities`)
  updateDate().then(() => { console.log(`[${dnow()}]: [UPDATE DONE]`); return {} })
})

