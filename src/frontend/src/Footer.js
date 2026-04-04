import React from "react";
import Link from "@mui/material/Link";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";

export default function Footer() {
    return (
        <Paper
            square={true}
            sx={{
                padding: 1,
                textAlign: "center",
                width: "100%",
                borderTop: "1px solid",
                borderColor: "primary.main",
            }}
        >
            <Typography variant="subtitle1">
                <Link href="https://github.com/respektive" underline="hover">
                    GitHub
                </Link>
                &nbsp;&#10022;&nbsp;
                <Link href="https://discord.gg/VZWRZZXcW4" underline="hover">
                    o!alt Discord
                </Link>
                &nbsp;&#10022;&nbsp;
                <Link href="https://osu.ppy.sh/users/1023489" underline="hover">
                    osu!
                </Link>
            </Typography>
        </Paper>
    );
}
