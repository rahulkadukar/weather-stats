const cron = require('node-cron')
const got = require('got')
const darkSkyToken = process.env['TOKEN_DARKSKY']
const { sqlQuery } = require('../utils/postgres')

Date.prototype.addDays = function(days) {
  const d = new Date(this.valueOf())
  d.setDate(d.getDate() + days)
  return d
}

async function sleepForMs(ms) { return new Promise(r => setTimeout(r, ms)) }
function dnow() { return new Date().toISOString() }

async function init() {
  const cityList = {
    'new_york': {
      'lat': '40.7128',
      'lon': '-74.0060',
      'name': 'New York'
    },
    'tokyo': {
      'lat': '35.6762',
      'lon': '139.6503',
      'name': 'Tokyo'
    }
  }

  await processCities(cityList)
}

async function processCities(cityList) {
  const dateRange = ['2020-01-01', '2020-01-31']
  const url = 'https://api.darksky.net/forecast'
  const maxCount = 10

  try {
    let c = 0
    for (const [k, v] of Object.entries(cityList)) {
      if (++c > maxCount) break
      const lat = v.lat
      const lon = v.lon
      const dateArray = []

      const data = await sqlQuery({
        text: `SELECT eventtime FROM "public".darksky_dailyweather WHERE cityname = $1`,
        values: [ k ]
      })

      const dateProcessed = {}
      data.dbResult.forEach((d) => { dateProcessed[d.eventtime] = 'X' })

      let firstDate = new Date(`${dateRange[0]}T06:00:00`)
      const lastDate = new Date(`${dateRange[1]}T06:00:00`)
      while (firstDate <= lastDate) {
        if (dateProcessed[firstDate.toISOString()] !== 'X') dateArray.push(firstDate)
        firstDate = firstDate.addDays(1)
      }

      for (let i = 0; i < dateArray.length; ++i) {
        const dateToProcess = dateArray[i].getTime() / 1000
        const dataResp = await got(`${url}/${darkSkyToken}/${lat},${lon},${dateToProcess}`)
        const weatherData = JSON.parse(dataResp.body)
        const insertData = {
          text: `INSERT INTO "public".darksky_dailyweather` +
              `(cityname, eventtime, rawdata)	VALUES ($1, $2, $3)`,
          values: [k, new Date(dateToProcess * 1000), JSON.stringify(weatherData)]
        }
        await sqlQuery(insertData)
        await sleepForMs(1000)

        console.log(`[${dnow()}]: Processed [${k}] for date ${new Date(dateToProcess * 1000)}`)
        if (++c > maxCount) break
      }
    }
  } catch (excp) {
    console.log(`[EXCEPTION]: ${excp.message}`)
  } finally {
    // Do nothing
  }
}

cron.schedule('0 */15 * * * *', () => {
  console.log(`[${dnow()}]: Starting DarkSky Fetch`);
  init().then(() => { console.log(`[${dnow()}]: DONE`); return {} })
});
