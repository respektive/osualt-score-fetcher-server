import React from "react";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Link from "@mui/material/Link";
import Footer from "./Footer";

const BASE_URL = "https://osualtv2.respektive.pw";
const oauth_url = `https://osu.ppy.sh/oauth/authorize?client_id=37221&redirect_uri=${BASE_URL}/api/oauth&response_type=code&scope=identify%20public`;

export default function Home() {
    return (
        <>
            <Grid container spacing={0} align="center" justify="center" direction="column">
                <Grid item sx={{ pt: 10, pl: 25, pr: 25 }}>
                    <Paper sx={{ padding: 5 }}>
                        <Typography variant="h4">Click this button to start fetching your scores:</Typography>
                        <Button variant="contained" href={oauth_url}>
                            Login with osu!
                        </Button>

                        <Typography variant="h6">
                            Upon logging in with osu! you get redirected to the{" "}
                            <Link href="/status" underline="hover">
                                status page
                            </Link>
                            . You can see the users currently being fetched and users already done.
                        </Typography>

                        <Typography variant="h6">
                            You are not able to fetch multiple times. Make sure to register in the Discord Bot so any new scores
                            will be fetched automatically.
                        </Typography>

                        <Typography variant="h6">
                            If there are any issues with missing scores in the bot you can ping respektive on Discord.
                        </Typography>
                    </Paper>
                </Grid>
            </Grid>

            <Footer />
        </>
    );
}
