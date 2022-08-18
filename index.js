const express = require('express')
const cors = require('cors')
const { spawn } = require('node:child_process')
const fetch = require('node-fetch')
const mysql = require('mysql')
const util = require('util')
const config = require('./config.json')

const connection = mysql.createPool(config.MYSQL)
const runSql = util.promisify(connection.query).bind(connection)

const app = express()
const port = config.PORT

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

  const python = spawn('python', ['fetch.py', token], {
    detached: true,
    stdio: [ 'ignore' ]
  });
  python.stdout.on('data', (data) => {
      console.log(`stdout: ${data}`);
    });
  python.stderr.on('data', (data) => {
      console.error(`stderr: ${data}`);
    });
  python.on('close', (code) => {
      console.log(`child process exited with code ${code}`);
    });
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
