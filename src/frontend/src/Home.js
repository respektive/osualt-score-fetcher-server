import { useState } from "react";
import { Box, Stack, Paper, Typography, Button, Link, TextField } from "@mui/material";
import { Link as RouterLink } from "react-router-dom";

const BASE_URL = process.env.REACT_APP_BASE_URL || "https://osualtv2.respektive.pw";
const OAUTH_URL = `https://osu.ppy.sh/oauth/authorize?client_id=37221&redirect_uri=${BASE_URL}/api/oauth&response_type=code&scope=identify%20public`;

export default function Home() {
    const [userId, setUserId] = useState("");
    const [error, setError] = useState(false);
    const [helperText, setHelperText] = useState("");

    const handleUserIdChange = (e) => {
        const value = e.target.value;
        setUserId(value);

        // user id has to be a positive integer
        const isNumber = value === "" || /^\d+$/.test(value);
        const isWithinRange = value === "" || parseInt(value) <= 2147483647;

        if (!isNumber) {
            setError(true);
            setHelperText("Numbers only");
        } else if (!isWithinRange) {
            setError(true);
            setHelperText("User ID is too large");
        } else {
            setError(false);
            setHelperText("");
        }
    };

    const state = { user_id: userId };
    const encodedState = btoa(JSON.stringify(state));
    const stateParam = userId && !error ? `&state=${encodedState}` : "";

    const fullUrl = `${OAUTH_URL}${stateParam}`;

    return (
        <>
            <Box display="flex" justifyContent="center" sx={{ mt: 4, px: 2, pb: 4 }}>
                <Paper
                    elevation={2}
                    sx={{
                        p: 3,
                        maxWidth: 1000,
                        width: "100%",
                    }}
                >
                    <Stack spacing={2.5}>
                        <Box>
                            <Typography variant="h5" fontWeight="600" gutterBottom>
                                o!alt Scorefetcher
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                                Authorize with osu! to sync scores with the{" "}
                                <strong style={{ textWrap: "nowrap" }}>osu!alternative</strong> Discord bot.
                            </Typography>
                        </Box>

                        <Box
                            sx={{
                                bgcolor: "#ffffff1a",
                                p: 2,
                                borderRadius: 1,
                                borderLeft: "4px solid",
                                borderColor: "primary.main",
                            }}
                        >
                            <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 0.5 }}>
                                A few things to note:
                            </Typography>
                            <Typography variant="body1" component="div" sx={{ lineHeight: 1.6 }}>
                                <ul style={{ margin: 0, paddingLeft: "1.2rem" }}>
                                    <li>
                                        You can enter a <strong>User ID</strong> or leave it blank to use your own.
                                    </li>
                                    <li>
                                        The user must be <strong>registered</strong> in the Discord bot first.
                                    </li>
                                    <li>
                                        Once started, you can track the progress on the{" "}
                                        <Link component={RouterLink} to="/status" underline="always">
                                            status page
                                        </Link>
                                        .
                                    </li>
                                </ul>
                            </Typography>
                        </Box>

                        <Stack direction="row" spacing={2}>
                            <TextField
                                label="osu! User ID"
                                placeholder="e.g. 39828"
                                size="small"
                                sx={{ flexGrow: 1 }}
                                value={userId}
                                onChange={handleUserIdChange}
                                error={error}
                                helperText={error ? helperText : ""}
                            />
                            <Button
                                variant="contained"
                                component="a"
                                href={error ? "#" : fullUrl}
                                disabled={error}
                                sx={{
                                    height: 40,
                                    textTransform: "none",
                                    lineHeight: 1,
                                    p: 1,
                                    minWidth: 160,
                                }}
                                disableElevation
                            >
                                Authorize with osu!
                            </Button>
                        </Stack>
                    </Stack>
                </Paper>
            </Box>
        </>
    );
}
