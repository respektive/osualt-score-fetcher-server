const { getModsEnum } = require("./helpers.js")

class OsuScore {
    constructor(beatmapScore) {
        const mods = beatmapScore["score"]["mods"]

        this.beatmap_id = beatmapScore["score"]["beatmap_id"]
        this.user_id = beatmapScore["score"]["user"]["id"]
        this.mods = mods
        this.score = beatmapScore["score"]["total_score"]             
        this.count300 = beatmapScore["score"]["statistics"]["great"] ?? 0
        this.count100 = beatmapScore["score"]["statistics"]["ok"] ?? 0
        this.count50 = beatmapScore["score"]["statistics"]["meh"] ?? 0
        this.countmiss = beatmapScore["score"]["statistics"]["miss"] ?? 0
        this.combo = beatmapScore["score"]["max_combo"]
        this.perfect = Number(beatmapScore["score"]["legacy_perfect"])
        this.enabled_mods = getModsEnum(mods.map(x => x.acronym))
        this.date_played = beatmapScore["score"]["ended_at"]
        this.rank = beatmapScore["score"]["rank"]
        this.pp = beatmapScore["score"]["pp"] ?? 0
        this.replay_available = Number(beatmapScore["score"]["replay"])
        this.is_hd = mods.map(x => x.acronym).includes("HD")
        this.is_hr = mods.map(x => x.acronym).includes("HR")
        this.is_dt = mods.map(x => x.acronym).includes("DT") || mods.map(x => x.acronym).includes("NC")
        this.is_fl = mods.map(x => x.acronym).includes("FL")
        this.is_ht = mods.map(x => x.acronym).includes("HT")
        this.is_ez = mods.map(x => x.acronym).includes("EZ")
        this.is_nf = mods.map(x => x.acronym).includes("NF")
        this.is_nc = mods.map(x => x.acronym).includes("NC")
        this.is_td = mods.map(x => x.acronym).includes("TD")
        this.is_so = mods.map(x => x.acronym).includes("SO")
        this.is_sd = mods.map(x => x.acronym).includes("SD") || mods.map(x => x.acronym).includes("PF")
        this.is_pf = mods.map(x => x.acronym).includes("PF")
    }

    getInsert() {
        const queryText = `insert into scores (user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
on conflict on constraint scores_pkey do update set score = excluded.score, count300 = EXCLUDED.count300, 
count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, 
perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, 
pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available, is_hd = EXCLUDED.is_hd, is_hr = EXCLUDED.is_hr, 
is_dt = EXCLUDED.is_dt, is_fl = EXCLUDED.is_fl, is_ht = EXCLUDED.is_ht, is_ez = EXCLUDED.is_ez, is_nf = EXCLUDED.is_nf, 
is_nc = EXCLUDED.is_nc, is_td = EXCLUDED.is_td, is_so = EXCLUDED.is_so, is_sd = EXCLUDED.is_sd, is_pf = EXCLUDED.is_pf`
        
        const query = {
            text: queryText,
            values: [this.user_id, this.beatmap_id, this.score, this.count300, this.count100, this.count50, this.countmiss,
                this.combo, this.perfect, this.enabled_mods, this.date_played, this.rank, this.pp, this.replay_available,
                this.is_hd, this.is_hr, this.is_dt, this.is_fl, this.is_ht, this.is_ez, this.is_nf, this.is_nc, this.is_td, this.is_so, this.is_sd, this.is_pf],
        }

        return query
    }
}

module.exports = OsuScore