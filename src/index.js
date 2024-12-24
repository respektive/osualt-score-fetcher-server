const express = require('express')
const cors = require('cors')
const { Worker, workerData } = require('node:worker_threads')
const path = require('node:path')
const fetch = require('node-fetch')
const mysql = require('mysql')
const util = require('util')
const config = require('../config.json')

const connection = mysql.createPool(config.MYSQL)
const runSql = util.promisify(connection.query).bind(connection)

const app = express()
const port = config.PORT

const MAX_ACTIVE = config.MAX_ACTIVE || 2;
let currentActive = 0;

const queue = [];

function processQueue() {
  if (currentActive >= MAX_ACTIVE || queue.length == 0)
    return;

  const [user] = queue.splice(0, 1);
  currentActive++;

  const worker = new Worker(path.resolve(__dirname, 'fetcher.js'), { workerData: user })

  worker.on('message', (msg) => console.log(msg))
  worker.on('error', (err) => console.error(err))
  worker.on('exit', (code) => {
    currentActive = Math.max(0, currentActive - 1)
    processQueue();
    if (code !== 0)
      console.log(`Worker stopped with exit code ${code}`)
  })
}

function addToQueue(user) {
  queue.push(user);
  processQueue();
}

async function getToken(code) {
  let res = await fetch("https://osu.ppy.sh/oauth/token", {
    method: 'post',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
        "grant_type": "authorization_code",
        "client_id": 10081,
        "client_secret": config.CLIENT_SECRET,
        "redirect_uri": "https://osualt.respektive.pw/api/oauth",
        "code": code
    })
})
.then(response => {
    return response.json();
});
if(res.access_token) {
    return res.access_token;
} else {
    return "invalid code";
}
}

async function getUserID(token) {
  const me = await fetch("https://osu.ppy.sh/api/v2/me/osu", {
    method: 'get',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    }
  })
  .then(response => {
      return response.json();
  })

  return me
}

app.use(cors({
    origin: '*'
}));

app.get('/oauth', async function (req, res) {
  let code = "";
  let token = "";
  let me = "";
  let user_id = "";
  if (!req.query.code){
    res.send("No code received")
    return
  }
  code = req.query.code
  token = await getToken(code).catch();
  me = await getUserID(token).catch();
  user_id = me.id;

  const fetched = await runSql("SELECT * FROM fetched_users WHERE user_id = ?", user_id)
  const fetching = await runSql("SELECT * FROM queue WHERE user_id = ?", user_id)

  if (fetched.length > 0 && fetched[0].updated_at != null && fetched[0].updated_at > new Date(+new Date - 12096e5)) {
    console.log("User already fetched recently")
  } else if (fetching.length > 0) {
    console.log("User already fetching")
  } else {
    console.log("User not fetched recently")
    
    addToQueue({
      access_token: token, 
      user_id: user_id, 
      username: me.username,
       most_played_count: 
       me.beatmap_playcounts_count
    })
  }

  console.log(token);

  res.redirect('https://osualt.respektive.pw/status')

  console.log(user_id);
  console.log(me.username);
});

app.get('/current', async function (req, res) {
  const current = await runSql("SELECT * FROM queue")
  res.send(current);
});

app.get('/fetched', async function (req, res) {
  const fetched = await runSql("SELECT * FROM fetched_users")
  res.send(fetched);
});

app.get('/', async function (req, res) {
  res.redirect('https://osualt.respektive.pw/')
});

app.listen(port, () => {
  console.log(`app listening at http://localhost:${port}`)
});
