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
                    variant="h6"
                    component={RouterLink}
                    to="/"
                    sx={{
                        textDecoration: "none",
                        color: "#e0e0e0",
                        pr: 4,
                        fontWeight: 700,
                        textWrap: "nowrap",
                    }}
                >
                    o!alt Scorefetcher
                </Typography>

                <Box sx={{ display: "flex", gap: 1 }}>
                    <Button
                        component={RouterLink}
                        to="/"
                        sx={{
                            fontWeight: isActive("/") ? 700 : 400,
                            color: isActive("/") ? "primary.main" : "inherit",
                            textTransform: "none",
                            fontSize: "0.9rem",
                        }}
                    >
                        Home
                    </Button>
                    <Button
                        component={RouterLink}
                        to="/status"
                        sx={{
                            fontWeight: isActive("/status") ? 700 : 400,
                            color: isActive("/status") ? "primary.main" : "inherit",
                            textTransform: "none",
                            fontSize: "0.9rem",
                        }}
                    >
                        Status
                    </Button>
                </Box>
            </Toolbar>
        </AppBar>
    );
}
