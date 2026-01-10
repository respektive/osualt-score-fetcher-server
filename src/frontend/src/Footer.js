import React from "react";
import Link from "@mui/material/Link";
import Paper from "@mui/material/Paper";
import Grid from "@mui/material/Grid";
import Typography from "@mui/material/Typography";

export default function Footer() {
    return (
        <Grid container spacing={3}>
            <Grid item xs={12} sm={12}>
                <Paper
                    eleveation={4}
                    square={true}
                    sx={{ padding: "5px", textAlign: "center", position: "fixed", bottom: 0, width: "100%" }}
                >
                    <Typography variant="subtitle1">
                        <Link href="https://github.com/respektive" underline="hover">
                            GitHub
                        </Link>
                        &nbsp;&#10022;&nbsp;
                        <Link href="https://osu.ppy.sh/users/1023489" underline="hover">
                            osu!
                        </Link>
                        &nbsp;&#10022;&nbsp;
                        <Link href="https://discord.gg/VZWRZZXcW4" underline="hover">
                            o!alt Discord
                        </Link>
                        &nbsp;&#10022;&nbsp;
                        <Link href="/" underline="hover">
                            Home
                        </Link>
                        &nbsp;&#10022;&nbsp;
                        <Link href="/status" underline="hover">
                            Status
                        </Link>
                    </Typography>
                </Paper>
            </Grid>
        </Grid>
    );
}
