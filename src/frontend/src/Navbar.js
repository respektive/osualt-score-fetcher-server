import React from "react";
import { AppBar, Toolbar, Typography, Box, Button } from "@mui/material";
import { Link as RouterLink, useLocation } from "react-router-dom";

export default function Navbar() {
    const location = useLocation();

    const isActive = (path) => location.pathname === path;

    return (
        <AppBar
            position="static"
            sx={{
                borderBottom: "1px solid",
                borderColor: "primary.main",
            }}
        >
            <Toolbar sx={{ justifyContent: "flex-start" }}>
                <Typography
                    variant="h5"
                    component={RouterLink}
                    to="/"
                    sx={{
                        textDecoration: "none",
                        color: "inherit",
                        pr: 4,
                        fontWeight: 700,
                    }}
                >
                    o!alt Scorefetcher
                </Typography>

                <Box sx={{ display: "flex", gap: 1 }}>
                    <Button
                        component={RouterLink}
                        to="/"
                        color="inherit"
                        sx={{
                            fontWeight: isActive("/") ? 700 : 400,
                            color: isActive("/") ? "primary.main" : "inherit",
                            textTransform: "none",
                        }}
                    >
                        Home
                    </Button>
                    <Button
                        component={RouterLink}
                        to="/status"
                        color="inherit"
                        sx={{
                            fontWeight: isActive("/status") ? 700 : 400,
                            color: isActive("/status") ? "primary.main" : "inherit",
                            textTransform: "none",
                        }}
                    >
                        Status
                    </Button>
                </Box>
            </Toolbar>
        </AppBar>
    );
}
