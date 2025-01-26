const { parentPort, workerData } = require("node:worker_threads");
const mysql = require("mysql");
const { Client } = require("pg");
const util = require("util");
const axios = require("axios").default;
const config = require("../config.json");
const OsuScore = require("./OsuScore.js");

const token_data = JSON.parse(workerData.data);
const most_played_count = token_data.most_played_count;
let access_token = token_data.access_token;

const api = axios.create({
    baseURL: "https://osu.ppy.sh/api/v2",
    headers: {
        Authorization: `Bearer ${access_token}`,
        "x-api-version": 20220707,
    },
});

const connection = mysql.createPool(config.MYSQL);
const runSql = util.promisify(connection.query).bind(connection);

let beatmapScores = [];
let beatmapIds = [];

async function getMostPlayedBeatmaps(offset = 0, retries = 0) {
    const osuAPIStartTime = process.hrtime();
    let response;
    try {
        response = await api.get(`/users/${workerData.user_id}/beatmapsets/most_played?limit=100&offset=${offset}`);
        const osuAPIEndTime = process.hrtime(osuAPIStartTime);
        const osuAPIDuration = osuAPIEndTime[0] + osuAPIEndTime[1] / 1e9;

        parentPort.postMessage({
            type: "osu_metrics",
            data: {
                osuAPIDuration: osuAPIDuration,
                route: "/users/:user_id/beatmapsets/most_played",
                statusCode: response.status,
            },
        });
    } catch (error) {
        const osuAPIEndTime = process.hrtime(osuAPIStartTime);
        const osuAPIDuration = osuAPIEndTime[0] + osuAPIEndTime[1] / 1e9;

        parentPort.postMessage({
            type: "osu_metrics",
            data: {
                osuAPIDuration: osuAPIDuration,
                route: "/users/:user_id/beatmapsets/most_played",
                statusCode: error.response?.status || 500,
            },
        });

        console.log(error);
        if (retries < 3) {
            await new Promise((resolve) => setTimeout(resolve, 1000 * Math.pow(2, retries)));
            await getMostPlayedBeatmaps(offset, retries + 1);
        }
        return;
    }
    let beatmaps = response.data;

    for (let i = 0; i < beatmaps.length; i++) {
        if (["ranked", "approved", "loved"].includes(beatmaps[i].beatmap?.status) && beatmaps[i].beatmap?.mode == "osu") {
            beatmapIds.push(beatmaps[i].beatmap_id);
        }
    }

    let progress = `Getting most played beatmaps... (${beatmapIds.length}/${most_played_count})`;
    let data = {
        beatmap_ids: beatmapIds,
        ...token_data,
    };
    await runSql("UPDATE queue SET progress = ?, data = ? WHERE user_id = ?", [
        progress,
        JSON.stringify(data),
        workerData.user_id,
    ]);

    if (beatmaps.length == 100) {
        offset += 100;
        await getMostPlayedBeatmaps(offset);
    }

    return;
}

async function getBeatmapsAmount() {
    const response = await axios.get("https://osu.respektive.pw/amount");
    const amount = response.data[0]["loved+ranked"];
    return amount;
}

async function getBeatmaps() {
    const response = await axios.get("https://osu.respektive.pw/beatmaps");
    const beatmaps = response.data;
    beatmapIds = beatmaps.ranked.beatmaps.concat(beatmaps.loved.beatmaps);
    return;
}

async function validToken(user_id) {
    let response;
    try {
        response = await api.get(`/users/${user_id}`);
    } catch (error) {
        console.log(error);
        await refreshToken();
        response = await api.get(`/users/${user_id}`);
    }
    let json = response.data;
    if ("error" in json || "authentication" in json) {
        console.log("Invalid token");
        return false;
    }

    console.log("Valid token");
    return true;
}

async function refreshToken() {
    let response;
    try {
        response = await axios.post("https://osu.ppy.sh/oauth/token", {
            grant_type: "refresh_token",
            client_id: 37221,
            client_secret: config.CLIENT_SECRET,
            refresh_token: token_data.refresh_token,
        });
    } catch (error) {
        console.log(error);
        console.log("Failed to refresh token");

        // todo: actually handle this
        process.exit();
    }
    access_token = response.data.access_token;
    return response;
}

async function insertScores(scores) {
    const osu_scores = scores.map((score) => new OsuScore(score));
    const values = osu_scores
        .map((score) => {
            const {
                user_id,
                beatmap_id,
                score: scoreValue,
                count300,
                count100,
                count50,
                countmiss,
                combo,
                perfect,
                enabled_mods,
                date_played,
                rank,
                pp,
                replay_available,
                is_hd,
                is_hr,
                is_dt,
                is_fl,
                is_ht,
                is_ez,
                is_nf,
                is_nc,
                is_td,
                is_so,
                is_sd,
                is_pf,
            } = score;
            return `(${user_id}, ${beatmap_id}, ${scoreValue}, ${count300}, ${count100}, ${count50}, ${countmiss}, ${combo}, ${perfect}, ${enabled_mods}, '${date_played}', '${rank}', ${pp}, ${replay_available}, ${is_hd}, ${is_hr}, ${is_dt}, ${is_fl}, ${is_ht}, ${is_ez}, ${is_nf}, ${is_nc}, ${is_td}, ${is_so}, ${is_sd}, ${is_pf})`;
        })
        .join(",");
    const scores_mods_values = osu_scores
        .map(
            (score) =>
                `(${score.user_id}, ${score.beatmap_id}, '${JSON.stringify(score.mods)}', '${
                    score.date_played
                }', '${JSON.stringify(score.statistics)}', '${JSON.stringify(score.maximum_statistics)}')`
        )
        .join(",");

    const query = `
      INSERT INTO scores (user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf)
      VALUES ${values}
      ON CONFLICT ON CONSTRAINT scores_pkey DO UPDATE SET 
        score = EXCLUDED.score,
        count300 = EXCLUDED.count300,
        count100 = EXCLUDED.count100,
        count50 = EXCLUDED.count50,
        countmiss = EXCLUDED.countmiss,
        combo = EXCLUDED.combo,
        perfect = EXCLUDED.perfect,
        enabled_mods = EXCLUDED.enabled_mods,
        date_played = EXCLUDED.date_played,
        rank = EXCLUDED.rank,
        pp = EXCLUDED.pp,
        replay_available = EXCLUDED.replay_available,
        is_hd = EXCLUDED.is_hd,
        is_hr = EXCLUDED.is_hr,
        is_dt = EXCLUDED.is_dt,
        is_fl = EXCLUDED.is_fl,
        is_ht = EXCLUDED.is_ht,
        is_ez = EXCLUDED.is_ez,
        is_nf = EXCLUDED.is_nf,
        is_nc = EXCLUDED.is_nc,
        is_td = EXCLUDED.is_td,
        is_so = EXCLUDED.is_so,
        is_sd = EXCLUDED.is_sd,
        is_pf = EXCLUDED.is_pf
    `;
    const scores_mods_query = `
      INSERT INTO scoresmods (user_id, beatmap_id, mods, date_played, statistics, maximum_statistics)
      VALUES ${scores_mods_values}
      ON CONFLICT ON CONSTRAINT scoresmods_pkey DO UPDATE SET 
        mods = EXCLUDED.mods,
        date_played = EXCLUDED.date_played,
        statistics = EXCLUDED.statistics,
        maximum_statistics = EXCLUDED.maximum_statistics`;

    const maxRetries = 3;
    let retries = 0;
    let insertedCount = 0;

    while (retries < maxRetries) {
        const batchClient = new Client(config.POSTGRES);

        try {
            const insertStartTime = process.hrtime();

            await batchClient.connect(); // Open a new connection
            const result = await batchClient.query(query);
            await batchClient.query(scores_mods_query);

            const insertEndTime = process.hrtime(insertStartTime);
            const duration = insertEndTime[0] + insertEndTime[1] / 1e9;

            parentPort.postMessage({
                type: "db_metrics",
                data: {
                    dbQueryDuration: duration,
                    query: "insertScores",
                },
            });

            insertedCount = result.rowCount;
            console.log(`${insertedCount} row(s) inserted`);
            beatmapScores.splice(0);
            break; // Success, exit the loop
        } catch (e) {
            console.error("Error inserting scores into PostgreSQL database:", e, query, scores_mods_query);
            retries++;
        } finally {
            await batchClient.end(); // Close the connection
        }

        // Wait before retrying
        await new Promise((resolve) => setTimeout(resolve, 2000));
    }
}

async function fetchScores() {
    let counter = 0;

    const batchSize = 100;
    for (const beatmap_id of beatmapIds) {
        // subtract batchSize from skip count to account for scores that have not been inserted due to process crashing before inserting
        if (Math.max(0, Number(workerData.skip) - batchSize) > counter) {
            counter++;
            continue;
        }
        const osuAPIStartTime = process.hrtime();
        let beatmapScore;
        try {
            const response = await api.get(`/beatmaps/${beatmap_id}/scores/users/${workerData.user_id}`);
            beatmapScore = response.data;

            const osuAPIEndTime = process.hrtime(osuAPIStartTime);
            const osuAPIDuration = osuAPIEndTime[0] + osuAPIEndTime[1] / 1e9;

            parentPort.postMessage({
                type: "osu_metrics",
                data: {
                    osuAPIDuration: osuAPIDuration,
                    route: "/beatmaps/:beatmap_id/scores/users/:user_id",
                    statusCode: response.status,
                },
            });
        } catch (error) {
            beatmapScore = { error: "null" };
            const osuAPIEndTime = process.hrtime(osuAPIStartTime);
            const osuAPIDuration = osuAPIEndTime[0] + osuAPIEndTime[1] / 1e9;
            parentPort.postMessage({
                type: "osu_metrics",
                data: {
                    osuAPIDuration: osuAPIDuration,
                    route: "/beatmaps/:beatmap_id/scores/users/:user_id",
                    statusCode: error.response?.status || 500,
                },
            });
        }

        if (!("error" in beatmapScore)) {
            beatmapScores.push(beatmapScore);
        }

        counter++;
        let progress = `Fetching Scores... (${counter}/${beatmapIds.length})`;
        let percentage = (counter / beatmapIds.length) * 100;
        try {
            await runSql("UPDATE queue SET progress = ?, percentage = ?, skip = ? WHERE user_id = ?", [
                progress,
                percentage,
                counter,
                workerData.user_id,
            ]);
        } catch (error) {
            console.error("Error updating queue table in MySQL database:", error);
        }

        if (beatmapScores.length >= batchSize || counter === beatmapIds.length) {
            await insertScores(beatmapScores);
        }
    }

    return;
}

async function main() {
    console.log("Starting fetch for user:", workerData.user_id);
    if (await validToken(workerData.user_id)) {
        console.log("Valid Token");
        // refresh token if user waited in queue for more than 12 hours, just to be sure the token doesn't expire mid-fetch
        if (workerData.date_added !== null && workerData.date_added < new Date(Date.now() - 1000 * 60 * 60 * 12)) {
            console.log("refreshing token");
            await refreshToken();
        }

        console.log("deleting from fetched users");
        await runSql("delete from fetched_users where user_id = ?", [workerData.user_id]);

        console.log("updating progress");
        let progress = "Getting Beatmap IDs...";
        await runSql("UPDATE queue SET progress = ? WHERE user_id = ?", [progress, workerData.user_id]);

        const beatmapsAmount = await getBeatmapsAmount();
        const requestsNeeded = Math.ceil(most_played_count / 100) + most_played_count;

        if (token_data.beatmap_ids) {
            beatmapIds = token_data.beatmap_ids;
        } else {
            // old users don't seem to have their most played beatmaps list populated correctly
            // see users like SiLviA for example who have a combined grade count of over 4500 but have about 3000 most played beatmaps
            // https://osu.ppy.sh/users/409747
            // this user_id cut off is an educated guess
            if (requestsNeeded > beatmapsAmount || workerData.user_id < 4000000) {
                console.log("Fetching all beatmaps...");
                await getBeatmaps();
            } else {
                console.log("Fetching most played beatmaps...");
                try {
                    await getMostPlayedBeatmaps();
                } catch (error) {
                    console.log(error);
                    await getMostPlayedBeatmaps();
                }
            }
        }

        console.log("Fetching scores...");
        await fetchScores();

        // when everything is done fetching
        await runSql("INSERT INTO fetched_users (user_id, username) VALUES (?, ?)", [workerData.user_id, workerData.username]);
        await runSql("DELETE FROM queue WHERE user_id = ?", workerData.user_id);

        parentPort.postMessage("Done fetching.");
    } else {
        parentPort.postMessage("Invalid Token.");
    }

    process.exit();
}

main();
