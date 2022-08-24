const { parentPort, workerData } = require("node:worker_threads")
const mysql = require("mysql")
const { Client } = require("pg")
const util = require("util")
const axios = require("axios").default
const config = require("../config.json")
const OsuScore = require("./OsuScore.js")

const access_token = workerData.access_token

const api = axios.create({
    baseURL: "https://osu.ppy.sh/api/v2",
    headers: {
        Authorization: `Bearer ${access_token}`,
        "x-api-version": 20220707
    }
})

const connection = mysql.createPool(config.MYSQL)
const runSql = util.promisify(connection.query).bind(connection)

const client = new Client(config.POSTGRES)

let beatmapIds = []

async function getMostPlayedBeatmaps(offset = 0){
    const response = await api.get(`/users/${workerData.user_id}/beatmapsets/most_played?limit=100&offset=${offset}`);
    let beatmaps = response.data;

    for(let i = 0; i < beatmaps.length; i++){
        if(["ranked", "approved", "loved"].includes(beatmaps[i].beatmap.status) && beatmaps[i].beatmap.mode == "osu"){
            beatmapIds.push(beatmaps[i].beatmap_id);
        }
    }

    let progress = `Getting most played beatmaps... (${beatmapIds.length}/${workerData.most_played_count})`
    await runSql("UPDATE queue SET progress = ? WHERE user_id = ?", [progress, workerData.user_id])
    
    if(beatmaps.length == 100){
        offset += 100;
        await getBeatmaps(offset);
    }

    return
}

async function getBeatmaps() {
    const response = await axios.get("https://osu.respektive.pw/beatmaps");
    const beatmaps = response.data;
    beatmapIds = beatmaps.ranked.beatmaps.concat(beatmaps.loved.beatmaps)
    return
}

async function validToken() {
    const response = await api.get("/beatmaps/75/scores/users/1023489")
    let json = response.data
    if ("error" in json || "authentication" in json) {
        return false
    }
    return true
}

async function insertScore(beatmapScore) {
    const osu_score = new OsuScore(beatmapScore)
    const insert_query = osu_score.getInsert()

    await client.query(insert_query)
    return 
}

async function fetchScores() {
    let counter = 0
    for (const beatmap_id of beatmapIds) {
        let beatmapScore;
        try {
            const response = await api.get(`/beatmaps/${beatmap_id}/scores/users/${workerData.user_id}`)
            beatmapScore = response.data
        } catch (error) {
            beatmapScore = {"error": "null"}
        }

        if (!("error" in beatmapScore)) {
            await insertScore(beatmapScore)
        }

        counter++
        let progress = `Fetching Scores... (${counter}/${beatmapIds.length})`
        let percentage = counter / beatmapIds.length * 100
        await runSql("UPDATE queue SET progress = ?, percentage = ? WHERE user_id = ?", [progress, percentage, workerData.user_id])
    }

    return
}

async function main() {
    await client.connect()

    if (validToken()) {
        let progress = "Getting Beatmap IDs..."
        await runSql("insert into queue (user_id, username, progress) values (?, ?, ?)", [workerData.user_id, workerData.username, progress])
    
        await getBeatmaps()
        await fetchScores()

        // when everything is done fetching
        await runSql("INSERT INTO fetched_users (user_id, username) VALUES (?, ?)", [workerData.user_id, workerData.username])
        await runSql("DELETE FROM queue WHERE user_id = ?", workerData.user_id)

        parentPort.postMessage("Done fetching.")
    } else {
        parentPort.postMessage("Invalid Token.")
    }

    await client.end()
}

main()
