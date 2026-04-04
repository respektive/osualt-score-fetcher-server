import Status from "./Status";
import Home from "./Home";
import Navbar from "./Navbar";
import Footer from "./Footer";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import { Box } from "@mui/material";
import "@fontsource/comfortaa/300.css";
import "@fontsource/comfortaa/400.css";
import "@fontsource/comfortaa/500.css";
import "@fontsource/comfortaa/700.css";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";

const mainTheme = createTheme({
    palette: {
        mode: "dark",
        primary: {
            main: "#ff66aa",
        },
    },
    shape: {
        borderRadius: 5,
    },
    typography: {
        fontFamily: "Comfortaa",
    },
});

function App() {
    return (
        <ThemeProvider theme={mainTheme}>
            <CssBaseline />
            <BrowserRouter>
                <Box
                    sx={{
                        display: "flex",
                        flexDirection: "column",
                        minHeight: "100vh",
                    }}
                >
                    <Navbar />
                    <Box component="main" sx={{ flexGrow: 1 }}>
                        <Routes>
                            <Route index element={<Home />} />
                            <Route path="status" element={<Status />} />

                            <Route path="*" element={<Navigate to="/" replace />} />
                        </Routes>
                    </Box>
                    <Footer />
                </Box>
            </BrowserRouter>
        </ThemeProvider>
    );
}

export default App;
