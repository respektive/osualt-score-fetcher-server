const { parentPort, workerData } = require("node:worker_threads")
const mysql = require("mysql")
const { Client } = require("pg")
const util = require("util")
const axios = require("axios").default
const config = require("./config.json")

const access_token = workerData.access_token

const api = axios.create({
    baseURL: "https://osu.ppy.sh/api/v2",
    headers: {
        Authorization: `Bearer ${access_token}`,
        "x-api-version": 20220707
    }
});

const connection = mysql.createPool(config.MYSQL)
const runSql = util.promisify(connection.query).bind(connection)

const client = new Client(config.POSTGRES)

const mods_enum = {
    ""    : 0,
    "NF"  : 1,
    "EZ"  : 2,
    "TD"  : 4,
    "HD"  : 8,
    "HR"  : 16,
    "SD"  : 32,
    "DT"  : 64,
    "RX"  : 128,
    "HT"  : 256,
    "NC"  : 512,
    "FL"  : 1024,
    "AT"  : 2048,
    "SO"  : 4096,
    "AP"  : 8192,
    "PF"  : 16384,
    "4K"  : 32768,
    "5K"  : 65536,
    "6K"  : 131072,
    "7K"  : 262144,
    "8K"  : 524288,
    "FI"  : 1048576,
    "RD"  : 2097152,
    "LM"  : 4194304,
    "9K"  : 16777216,
    "10K" : 33554432,
    "1K"  : 67108864,
    "3K"  : 134217728,
    "2K"  : 268435456,
    "V2"  : 536870912
}

function getModsEnum(mods){
    let n = 0;
    if(mods.includes("NC")){
        mods.push("DT")
    }
    if(mods.includes("PF")){
        mods.push("SD")
    }
    for(let i = 0; i < mods.length; i++){
        n += mods_enum[mods[i]];
    }
    return n;
}

let beatmapIds = []

async function getBeatmaps(offset = 0){
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

async function validToken() {
    const response = await api.get("/beatmaps/75/scores/users/1023489")
    let json = response.data
    if ("error" in json || "authentication" in json) {
        return false
    }
    return true
}

async function insertScore(beatmapScore) {
    const beatmap_id = beatmapScore["score"]["beatmap_id"]
    const user_id = beatmapScore["score"]["user"]["id"]
    const mods = beatmapScore["score"]["mods"]
    const score = beatmapScore["score"]["score"]             
    const count300 = beatmapScore["score"]["statistics"]["count_300"]
    const count100 = beatmapScore["score"]["statistics"]["count_100"]
    const count50 = beatmapScore["score"]["statistics"]["count_50"]
    const countmiss = beatmapScore["score"]["statistics"]["count_miss"]
    const combo = beatmapScore["score"]["max_combo"]
    const perfect = Number(beatmapScore["score"]["legacy_perfect"])
    const enabled_mods = getModsEnum(mods.map(x => x.acronym))
    const date_played = beatmapScore["score"]["ended_at"]
    const rank = beatmapScore["score"]["rank"]
    const pp = beatmapScore["score"]["pp"] ?? 0
    const replay_available = Number(beatmapScore["score"]["replay"])
    const is_hd = "HD"in mods.map(x => x.acronym)
    const is_hr = "HR"in mods.map(x => x.acronym)
    const is_dt = ("DT"in mods.map(x => x.acronym) || "NC" in mods.map(x => x.acronym))
    const is_fl = "FL"in mods.map(x => x.acronym)
    const is_ht = "HT"in mods.map(x => x.acronym)
    const is_ez = "EZ"in mods.map(x => x.acronym)
    const is_nf = "NF"in mods.map(x => x.acronym)
    const is_nc = "NC"in mods.map(x => x.acronym)
    const is_td = "TD"in mods.map(x => x.acronym)
    const is_so = "SO"in mods.map(x => x.acronym)
    const is_sd = ("SD"in mods.map(x => x.acronym) || "PF" in mods.map(x => x.acronym))
    const is_pf = "PF"in mods.map(x => x.acronym)

    const queryText = `insert into scores (user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
on conflict on constraint scores_pkey do update set score = excluded.score, count300 = EXCLUDED.count300, 
count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, 
perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, 
pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available, is_hd = EXCLUDED.is_hd, is_hr = EXCLUDED.is_hr, 
is_dt = EXCLUDED.is_dt, is_fl = EXCLUDED.is_fl, is_ht = EXCLUDED.is_ht, is_ez = EXCLUDED.is_ez, is_nf = EXCLUDED.is_nf, 
is_nc = EXCLUDED.is_nc, is_td = EXCLUDED.is_td, is_so = EXCLUDED.is_so, is_sd = EXCLUDED.is_sd, is_pf = EXCLUDED.is_pf`

    const query = {
    text: queryText,
    values: [user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf],
    }

    await client.query(query)
    return 
}

async function fetchScores() {
    let counter = 0
    for (const beatmap_id in beatmapIds) {
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
        let progress = "Getting most played beatmaps..."
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
}

main()