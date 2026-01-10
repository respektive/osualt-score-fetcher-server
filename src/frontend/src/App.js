import Status from "./Status";
import Home from "./Home";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
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
                <Routes>
                    <Route index element={<Home />} />
                    <Route path="status" element={<Status />} />

                    <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
            </BrowserRouter>
        </ThemeProvider>
    );
}

export default App;
