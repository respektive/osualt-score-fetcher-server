import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Box, Typography, Paper, Snackbar, Alert, Stack, Divider, LinearProgress } from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";

const BASE_URL = process.env.REACT_APP_BASE_URL || "https://osualtv2.respektive.pw";

export default function Status() {
    const [searchParams, setSearchParams] = useSearchParams();
    const alertType = searchParams.get("alert");
    const id = searchParams.get("id");

    const [snackbarOpen, setSnackbarOpen] = useState(!!alertType);

    useEffect(() => {
        if (alertType) {
            window.history.replaceState({}, document.title, window.location.pathname);
        }
    }, [alertType]);

    const getAlertMessage = () => {
        if (alertType === "already_fetched") return `User already fetched. User ID: ${id}`;
        if (alertType === "already_fetching") return `User currently fetching. User ID: ${id}`;
        if (alertType === "queued") return `User added to fetching queue. User ID: ${id}`;

        return;
    };

    const handleSnackbarClose = (event, reason) => {
        if (reason === "clickaway") return;
        setSnackbarOpen(false);
    };

    const [current, setCurrent] = useState([]);
    const [fetched, setFetched] = useState([]);

    const fetchCurrentData = async () => {
        try {
            const currentRes = await fetch(`${BASE_URL}/api/current`);
            const currentJson = await currentRes.json();
            setCurrent(currentJson);
        } catch (error) {
            console.error("Error fetching current data:", error);
        }
    };

    const fetchFetchedData = async () => {
        try {
            const fetchedRes = await fetch(`${BASE_URL}/api/fetched`);
            const fetchedJson = await fetchedRes.json();
            fetchedJson.sort((a, b) => Intl.Collator().compare(a.username, b.username));
            setFetched(fetchedJson);
        } catch (error) {
            console.error("Error fetching fetched data:", error);
        }
    };

    useEffect(() => {
        fetchCurrentData();
        const currentInterval = setInterval(fetchCurrentData, 10000);

        return () => {
            clearInterval(currentInterval);
        };
    }, []);

    useEffect(() => {
        fetchFetchedData();
        const fetchedInterval = setInterval(fetchFetchedData, 60000);

        return () => {
            clearInterval(fetchedInterval);
        };
    }, []);

    const columns = [
        { field: "user_id", headerName: "User ID", width: 150 },
        { field: "username", headerName: "Username", width: 200 },
        { field: "registration_date", headerName: "Registration Date", width: 200 },
        { field: "updated_at", headerName: "Last Updated", width: 200 },
    ];

    const paginationModel = { page: 0, pageSize: 10 };
    const sortModel = [{ field: "updated_at", sort: "desc" }];

    return (
        <Box display="flex" justifyContent="center" sx={{ mt: 4, px: 2, pb: 4 }}>
            <Paper
                elevation={2}
                sx={{
                    p: 3,
                    maxWidth: 1000,
                    width: "100%",
                }}
            >
                <Stack spacing={4}>
                    <Box>
                        <Typography variant="h6" fontWeight="600" gutterBottom>
                            Currently Fetching
                        </Typography>
                        <Divider sx={{ mb: 2 }} />
                        <Box
                            sx={{
                                bgcolor: "#ffffff1a",
                                p: 2,
                                borderRadius: 1,
                                borderLeft: "4px solid",
                                borderColor: "primary.main",
                            }}
                        >
                            {current.length === 0 ? (
                                <Typography variant="body2" color="text.secondary">
                                    No active fetches in progress.
                                </Typography>
                            ) : (
                                <Stack spacing={2}>
                                    {current.map((user) => (
                                        <Box key={user.username} sx={{ pb: 1 }}>
                                            <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.5 }}>
                                                <Typography variant="body2" fontWeight="bold">
                                                    {user.username} | {user.progress}
                                                </Typography>
                                                <Typography variant="body2" color="text.secondary">
                                                    {user.percentage ? user.percentage.toFixed(2) : "0.0"}%
                                                </Typography>
                                            </Box>
                                            <LinearProgress
                                                variant="determinate"
                                                value={user.percentage}
                                                sx={{ height: 6, borderRadius: 2 }}
                                            />
                                        </Box>
                                    ))}
                                </Stack>
                            )}
                        </Box>
                    </Box>

                    <Box>
                        <Typography variant="h6" fontWeight="600" gutterBottom>
                            Recently Fetched
                        </Typography>
                        <Divider sx={{ mb: 2 }} />
                        <Paper sx={{ borderRadius: 1 }}>
                            <DataGrid
                                rows={fetched.map((user, index) => ({
                                    id: index,
                                    user_id: user.user_id,
                                    username: user.username,
                                    updated_at: user.updated_at,
                                    registration_date: user.registration_date,
                                }))}
                                columns={columns}
                                initialState={{ pagination: { paginationModel }, sorting: { sortModel } }}
                                pageSizeOptions={[5, 10, 15, 25, 50, 100]}
                                disableRowSelectionOnClick
                                sx={{ border: 0, maxHeight: 800 }}
                            />
                        </Paper>
                    </Box>
                </Stack>
            </Paper>

            <Snackbar
                anchorOrigin={{ vertical: "top", horizontal: "left" }}
                sx={{ mt: 8 }}
                open={snackbarOpen}
                autoHideDuration={10000}
                onClose={handleSnackbarClose}
            >
                <Alert onClose={handleSnackbarClose} severity={alertType !== "queued" ? "info" : "success"} variant="filled">
                    {getAlertMessage()}
                </Alert>
            </Snackbar>
        </Box>
    );
}
