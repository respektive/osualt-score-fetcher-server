const { Worker, workerData } = require('node:worker_threads')

token = ""

const worker = new Worker("./src/fetcher.js", { workerData: { access_token: token, user_id: 1023489, username: "respektive", most_played_count: 33481 } })

worker.on('message', (msg) => console.log(msg))
worker.on('error', (err) => console.error(err))
worker.on('exit', (code) => {
  if (code !== 0)
    console.log(`Worker stopped with exit code ${code}`)
})