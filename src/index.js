const express = require("express");
const cors = require("cors");
const path = require("path");
const config = require("../config.json");

const db = require("./db.js");

const app = express();
const port = config.PORT;

async function checkFetchedStatusForUser(user_id) {
    try {
        const result = await db.query("SELECT is_synced FROM registrations WHERE user_id = $1", [user_id]);
        return result.rows[0].is_synced;
    } catch (err) {
        console.error("Failed to check fetched status", err);
        return null;
    }
}

async function checkFetchingStatusForUser(user_id) {
    try {
        const result = await db.query(
            "SELECT * FROM logging WHERE logtype = 'FETCHER' AND data->>'user_id' = $1 ORDER BY entrytime DESC LIMIT 1",
            [user_id]
        );
        //console.log(result);

        const data = result.rows[0]?.data || null;
        if (!data) {
            return false;
        }

        return data.total != data.fetched;
    } catch (err) {
        console.error("Failed to check fetched status", err);
        return null;
    }
}

async function getFetchedUsers() {
    try {
        const result = await db.query(
            "SELECT user_id, username, registrations.lchg_time, registrationdate FROM registrations LEFT JOIN userlive USING (user_id) WHERE is_synced = true"
        );

        const fetched_users = result.rows.map((row) => {
            return {
                user_id: row.user_id,
                username: row.username || "🔍❓🤕",
                updated_at: row.lchg_time,
                registration_date: row.registrationdate,
            };
        });

        return fetched_users;
    } catch (err) {
        console.error("Failed to fetched users", err);
        return null;
    }
}

async function getFetchingUsers() {
    try {
        const result = await db.query(`
            SELECT DISTINCT ON (data->'user_id') data, data->'user_id' AS user_id, username, entrytime, message
            FROM logging LEFT JOIN userlive ON (logging.data->>'user_id')::int = userlive.user_id
            WHERE logtype = 'FETCHER' ORDER BY data->'user_id', entrytime DESC`);

        const fetching_users = result.rows
            .filter((row) => row.data.total != row.data.fetched)
            .map((row) => {
                return {
                    user_id: row.user_id,
                    username: row.username || "🔍❓🤕",
                    progress: `Fetching scores... (${row.data.fetched}/${row.data.total})`,
                    percentage: (row.data.fetched / row.data.total) * 100,
                };
            });

        return fetching_users;
    } catch (err) {
        console.error("Failed to fetched users", err);
        return null;
    }
}

async function addToQueue(token_data, user_id) {
    try {
        const result = await db.query("INSERT INTO tokens VALUES ($1, $2)", [token_data, user_id]);
        console.log(`Inserted ${result.rowCount} row(s)`);
    } catch (err) {
        console.error("Failed to insert user into queue", err);
    }
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
            redirect_uri: `${config.BASE_URL}/api/oauth`,
            code: code,
        }),
    }).then((response) => {
        return response.json();
    });
    //console.log(res);

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

app.use(express.static(path.join(__dirname, "frontend/build")));

app.get("/api/oauth", async function (req, res) {
    if (!req.query.code) {
        res.send("No code received");
        return;
    }
    let code = req.query.code;
    let token_data = await getToken(code).catch();
    let me = await getUserID(token_data?.access_token).catch();
    let user_id = me.id;

    if (!token_data || !user_id) {
        res.send("Failed to get token");
        return;
    }

    const is_fetched = await checkFetchedStatusForUser(user_id);
    const is_fetching = await checkFetchingStatusForUser(user_id);

    if (is_fetched == null || is_fetching == null) {
        res.send("Failed to check database for fetching status");
        return;
    }

    if (is_fetched) {
        console.log("User already fetched:", user_id, me.username);
    } else if (is_fetching) {
        console.log("User already fetching:", user_id, me.username);
    } else {
        console.log(token_data);
        console.log("Inserted token for user:", user_id, me.username);
        //await addToQueue(token_data, user_id);
    }

    res.redirect(`${config.BASE_URL}/status`);
});

app.get("/api/current", async function (req, res) {
    const fetchngUsers = await getFetchingUsers();
    res.send(fetchngUsers);
});

app.get("/api/fetched", async function (req, res) {
    const fetchedUsers = await getFetchedUsers();
    res.send(fetchedUsers);
});

app.get("*", (req, res) => {
    res.sendFile(path.join(__dirname, "frontend/build/index.html"));
});

app.listen(port, () => {
    console.log(`app listening at http://localhost:${port}`);
});
