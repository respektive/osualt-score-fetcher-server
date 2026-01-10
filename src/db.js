import { Pool } from "pg";
import config from "../config.json" with { type: "json" };

const pool = new Pool(config.POSTGRES);

export const query = (text, params) => {
    return pool.query(text, params);
}