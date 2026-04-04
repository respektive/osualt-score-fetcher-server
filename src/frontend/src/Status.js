import React, { useEffect, useState, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { Grid, LinearProgress, Typography, Paper, Snackbar, Alert } from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";
import Footer from "./Footer";

const BASE_URL = process.env.REACT_APP_BASE_URL || "https://osualtv2.respektive.pw";

export default function Status() {
    const intervalRef = useRef();
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

    const fetchData = async () => {
        const current = await fetch(`${BASE_URL}/api/current`);
        const currentJson = await current.json();
        setCurrent(currentJson);

        const fetched = await fetch(`${BASE_URL}/api/fetched`);
        const fetchedJson = await fetched.json();
        fetchedJson.sort((a, b) => Intl.Collator().compare(a.username, b.username));
        setFetched(fetchedJson);
    };

    useEffect(() => {
        fetchData();
        intervalRef.current = setInterval(fetchData, 10000);

        return () => {
            clearInterval(intervalRef.current);
        };
    }, []);

    const columns = [
        { field: "user_id", headerName: "User ID", width: 150 },
        { field: "username", headerName: "Username", width: 200 },
        { field: "registration_date", headerName: "Registration Date", width: 200 },
        { field: "updated_at", headerName: "Last Updated", width: 200 },
    ];

    const paginationModel = { page: 0, pageSize: 10 };
    const sortModel = { field: "updated_at", sort: "desc" };

    return (
        <>
            <Grid container align="center" justify="center" sx={{ padding: 5 }}>
                <Grid item xs={6} sx={{ padding: "10px" }}>
                    <Typography variant="h4">List of users currently being fetched:</Typography>
                    {current.map((user) => (
                        <Paper sx={{ width: "80%", mt: "10px" }}>
                            <Typography variant="h6" key={user.username}>
                                {user.username} | {user.progress} | {user.percentage ? user.percentage.toFixed(2) : "0.0"}%
                            </Typography>
                            <LinearProgress
                                sx={{ width: "80%" }}
                                variant="determinate"
                                value={user.percentage}
                                key={user.percentage}
                            />
                        </Paper>
                    ))}
                </Grid>

                <Grid item xs={6} sx={{ padding: "10px" }}>
                    <Typography variant="h4" sx={{ mb: "10px" }}>
                        List of users already done fetching:
                    </Typography>
                    <Grid container spacing={1}>
                        <DataGrid
                            rows={fetched.map((user, index) => ({
                                id: index,
                                user_id: user.user_id,
                                username: user.username,
                                updated_at: user.updated_at,
                                registration_date: user.registration_date,
                            }))}
                            columns={columns}
                            initialState={{ pagination: { paginationModel }, sorting: { sortModel: [sortModel] } }}
                            pageSizeOptions={[5, 10, 15, 25, 50, 100]}
                            disableRowSelectionOnClick
                            sx={{ border: 0 }}
                        />
                    </Grid>
                </Grid>
            </Grid>
            <Footer />

            <Snackbar
                anchorOrigin={{ vertical: "top", horizontal: "left" }}
                open={snackbarOpen}
                autoHideDuration={5000}
                onClose={handleSnackbarClose}
            >
                <Alert onClose={handleSnackbarClose} severity={alertType !== "queued" ? "info" : "success"} variant="filled">
                    {getAlertMessage()}
                </Alert>
            </Snackbar>
        </>
    );
}
