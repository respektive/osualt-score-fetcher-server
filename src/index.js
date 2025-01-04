const express = require("express");
const cors = require("cors");
const { Worker, workerData } = require("node:worker_threads");
const path = require("node:path");
const fetch = require("node-fetch");
const mysql = require("mysql");
const util = require("util");
const config = require("../config.json");

const connection = mysql.createPool(config.MYSQL);
const runSql = util.promisify(connection.query).bind(connection);

const app = express();
const port = config.PORT;

const MAX_ACTIVE = config.MAX_ACTIVE || 2;
let currentActive = 0;

async function resumeQueue() {
    const queue = await runSql("SELECT * FROM queue WHERE progress != 'Waiting in queue...'");

    for (const user of queue) {
        console.log(`Resuming ${user.username}...`);
        addToQueue(user);
        await new Promise((r) => setTimeout(r, 2000));
    }
}

async function processQueue() {
    const queue = await runSql("SELECT * FROM queue WHERE progress = 'Waiting in queue...' ORDER BY date_added ASC");

    if (currentActive >= MAX_ACTIVE || queue.length == 0) return;

    const [user] = queue.splice(0, 1);
    currentActive++;

    const worker = new Worker(path.resolve(__dirname, "fetcher.js"), { workerData: user });

    worker.on("message", (msg) => console.log(msg));
    worker.on("error", (err) => console.error(err));
    worker.on("exit", (code) => {
        currentActive = Math.max(0, currentActive - 1);
        processQueue();
        if (code !== 0) console.log(`Worker stopped with exit code ${code}`);
    });
}

async function addToQueue(user) {
    const progress = "Waiting in queue...";
    await runSql(
        "insert into queue (user_id, username, progress, data) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE progress = ?",
        [user.user_id, user.username, progress, JSON.stringify(user.data), progress]
    );

    processQueue();
}

async function getToken(code) {
    let res = await fetch("https://osu.ppy.sh/oauth/token", {
        method: "post",
        headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            grant_type: "authorization_code",
            client_id: 37221,
            client_secret: config.CLIENT_SECRET,
            redirect_uri: "https://osualt.respektive.pw/api/oauth",
            code: code,
        }),
    }).then((response) => {
        return response.json();
    });
    console.log(res);

    if (res.access_token) {
        return res;
    } else {
        console.log("Failed to get token");
        return null;
    }
}

async function getUserID(token) {
    const me = await fetch("https://osu.ppy.sh/api/v2/me/osu", {
        method: "get",
        headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
        },
    }).then((response) => {
        return response.json();
    });

    return me;
}

app.use(
    cors({
        origin: "*",
    })
);

app.get("/oauth", async function (req, res) {
    if (!req.query.code) {
        res.send("No code received");
        return;
    }
    let code = req.query.code;
    let token_data = await getToken(code).catch();
    let me = await getUserID(token_data.access_token).catch();
    let user_id = me.id;

    if (!token_data || !user_id) {
        res.send("Failed to get token");
        return;
    }

    const fetched = await runSql("SELECT * FROM fetched_users WHERE user_id = ?", user_id);
    const fetching = await runSql("SELECT * FROM queue WHERE user_id = ?", user_id);

    if (fetched.length > 0 && fetched[0].updated_at != null && fetched[0].updated_at > new Date(+new Date() - 12096e5)) {
        console.log("User already fetched recently");
    } else if (fetching.length > 0) {
        console.log("User already fetching");
    } else {
        console.log("User not fetched recently");

        addToQueue({
            data: {
                most_played_count: me.beatmap_playcounts_count,
                ...token_data,
            },
            user_id: user_id,
            username: me.username,
        });
    }

    console.log(token_data);

    res.redirect("https://osualt.respektive.pw/status");

    console.log(user_id);
    console.log(me.username);
});

app.get("/current", async function (req, res) {
    const current = await runSql("SELECT user_id, username, progress, percentage, date_added FROM queue");
    res.send(current);
});

app.get("/fetched", async function (req, res) {
    const fetched = await runSql("SELECT * FROM fetched_users");
    res.send(fetched);
});

app.get("/", async function (req, res) {
    res.redirect("https://osualt.respektive.pw/");
});

app.listen(port, () => {
    console.log(`app listening at http://localhost:${port}`);
});

resumeQueue();
