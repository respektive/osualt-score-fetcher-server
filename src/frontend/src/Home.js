import React, { useState } from "react";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Link from "@mui/material/Link";
import TextField from "@mui/material/TextField";
import Footer from "./Footer";

const BASE_URL = process.env.REACT_APP_BASE_URL || "https://osualtv2.respektive.pw";
const OAUTH_URL = `https://osu.ppy.sh/oauth/authorize?client_id=37221&redirect_uri=${BASE_URL}/api/oauth&response_type=code&scope=identify%20public`;

export default function Home() {
    const [userId, setUserId] = useState("");
    const [error, setError] = useState(false);

    const handleUserIdChange = (e) => {
        const value = e.target.value;
        setUserId(value);

        // user id has to be a positive integer
        if (value === "" || (/^\d+$/.test(value) && value <= 2147483647)) {
            setError(false);
        } else {
            setError(true);
        }
    };

    const state = { user_id: userId };
    const encodedState = btoa(JSON.stringify(state));
    const stateParam = userId && !error ? `&state=${encodedState}` : "";

    const fullUrl = `${OAUTH_URL}${stateParam}`;

    return (
        <>
            <Grid container spacing={0} align="center" justify="center" direction="column">
                <Grid item sx={{ pt: 10, pl: 25, pr: 25 }}>
                    <Paper sx={{ padding: 5 }}>
                        <Typography variant="h6">
                            Use this page to fetch yours or someone elses scores for the osu!alternative v2 discord bot.
                        </Typography>

                        <Typography variant="h6">
                            After authorizing with osu! you will get redirected to the{" "}
                            <Link href="/status" underline="hover">
                                status page
                            </Link>
                            . It shows any users currently being fetched and users already done.
                        </Typography>

                        <Typography variant="h6">
                            You are not able to fetch multiple times. Users need to be registered in the discord bot before
                            fetching.
                        </Typography>

                        <Typography variant="h6">
                            Enter a valid User ID in the field below, or leave it empty to use your own ID, then click the button
                            to authorize.
                        </Typography>

                        <Grid container direction="row" justifyContent="center" alignItems="baseline" spacing={2}>
                            <Grid item>
                                <TextField
                                    label="User ID"
                                    variant="outlined"
                                    type="text"
                                    margin="dense"
                                    size="small"
                                    value={userId}
                                    onChange={handleUserIdChange}
                                    error={error}
                                    helperText={error ? "Invalid User ID" : ""}
                                ></TextField>
                            </Grid>
                            <Grid item>
                                <Button
                                    variant="contained"
                                    disableElevation
                                    href={error ? "#" : fullUrl}
                                    disabled={error}
                                    sx={{ mt: 3.8 }}
                                >
                                    Authorize with osu!
                                </Button>
                            </Grid>
                        </Grid>
                    </Paper>
                </Grid>
            </Grid>
            <Footer />
        </>
    );
}
