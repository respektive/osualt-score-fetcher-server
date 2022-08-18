import requests
import psycopg2
import sys
import json
import mariadb

with open('config.json', 'r') as f:
    config = json.load(f)

# pg_conn = f"host={config['POSTGRES']['host']} dbname={config['POSTGRES']['database']} user={config['POSTGRES']['user']} password={config['POSTGRES']['password']}"

# conn = psycopg2.connect(pg_conn)
# conn.set_session(autocommit=True)
# cur = conn.cursor()

mariadb_conn = mariadb.connect(
    user=config['MYSQL']['user'],
    password=config['MYSQL']['password'],
    host=config['MYSQL']['host'],
    database=config['MYSQL']['database'],
    autocommit=True
)
mycur = mariadb_conn.cursor()

mods_enum = {
    ''    : 0,
    'NF'  : 1,
    'EZ'  : 2,
    'TD'  : 4,
    'HD'  : 8,
    'HR'  : 16,
    'SD'  : 32,
    'DT'  : 64,
    'RX'  : 128,
    'HT'  : 256,
    'NC'  : 512,
    'FL'  : 1024,
    'AT'  : 2048,
    'SO'  : 4096,
    'AP'  : 8192,
    'PF'  : 16384,
    '4K'  : 32768,
    '5K'  : 65536,
    '6K'  : 131072,
    '7K'  : 262144,
    '8K'  : 524288,
    'FI'  : 1048576,
    'RD'  : 2097152,
    'LM'  : 4194304,
    '9K'  : 16777216,
    '10K' : 33554432,
    '1K'  : 67108864,
    '3K'  : 134217728,
    '2K'  : 268435456,
    'V2'  : 536870912,
}

def getEnum(mods):
    enum = 0
    if 'NC' in mods:
        mods.append('DT')
    if 'PF' in mods:
        mods.append('SD')
    for mod in mods:
        enum += mods_enum[mod]
    
    return enum


session = requests.Session()
access_token = sys.argv[1]
session.headers.update({'Authorization': f'Bearer {access_token}'})
user = session.get('https://osu.ppy.sh/api/v2/me/osu').json()
userid = user['id']
username = user['username']
#beatmaps = requests.get('https://osu.respektive.pw/beatmaps').json()
#beatmapIds = beatmaps['ranked']['beatmaps']
beatmapIds = []

def getBeatmaps(offset = 0):
    response = session.get(f'https://osu.ppy.sh/api/v2/users/{userid}/beatmapsets/most_played?limit=100&offset={offset}')
    beatmaps = response.json()
    for map in beatmaps:
        if map['beatmap']['status'] in ('ranked', 'approved', 'loved') and map['beatmap']['mode'] == 'osu':
            beatmapIds.append(map['beatmap_id'])
    print(len(beatmapIds))
    if len(beatmaps) == 100:
        offset += 100
        getBeatmaps(offset)

def validToken():
    check = session.get(f'https://osu.ppy.sh/api/v2/beatmaps/75/scores/users/1023489').json()
    if not ('error' in check) or ('authentication' in check):
        return True
    else:
        print(check)
        return False

def fetchScores():
    counter = 0
    for beatmap_id in beatmapIds:
        try:
            response = session.get(f'https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}/scores/users/{userid}')
            beatmapScore = response.json()
        except:
            print("There was an unexpected error when reaching the osu api")
            beatmapScore = {"error": "null"}
            pass

        if not 'error' in beatmapScore:
            mods = beatmapScore['score']['mods']
            score = beatmapScore['score']['score']             
            count300 = beatmapScore['score']['statistics']['count_300']
            count100 = beatmapScore['score']['statistics']['count_100']
            count50 = beatmapScore['score']['statistics']['count_50']
            countmiss = beatmapScore['score']['statistics']['count_miss']
            combo = beatmapScore['score']['max_combo']
            perfect = int(beatmapScore['score']['perfect'] == True)
            enabled_mods = getEnum(mods)
            date_played = beatmapScore['score']['created_at']
            rank = beatmapScore['score']['rank']
            if beatmapScore['score']['pp']:
                pp = beatmapScore['score']['pp']
            else:
                pp = 0
            replay_available = int(beatmapScore['score']['replay'] == True)
            is_hd = 'HD'in mods
            is_hr = 'HR'in mods
            is_dt = 'DT'in mods or 'NC' in mods
            is_fl = 'FL'in mods
            is_ht = 'HT'in mods
            is_ez = 'EZ'in mods
            is_nf = 'NF'in mods
            is_nc = 'NC'in mods
            is_td = 'TD'in mods
            is_so = 'SO'in mods
            is_sd = 'SD'in mods or 'PF' in mods
            is_pf = 'PF'in mods

            query = f"""insert into scores values ({userid},{beatmap_id},{score},{count300},{count100},{count50},{countmiss},{combo},{perfect},{enabled_mods},'{date_played}','{rank}',{pp},{replay_available},{is_hd},{is_hr},{is_dt},{is_fl},{is_ht},{is_ez},{is_nf},{is_nc},{is_td},{is_so},{is_sd},{is_pf})
on conflict on constraint scores_pkey do update set score = excluded.score, count300 = EXCLUDED.count300, 
count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, 
perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, 
pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available, is_hd = EXCLUDED.is_hd, is_hr = EXCLUDED.is_hr, 
is_dt = EXCLUDED.is_dt, is_fl = EXCLUDED.is_fl, is_ht = EXCLUDED.is_ht, is_ez = EXCLUDED.is_ez, is_nf = EXCLUDED.is_nf, 
is_nc = EXCLUDED.is_nc, is_td = EXCLUDED.is_td, is_so = EXCLUDED.is_so, is_sd = EXCLUDED.is_sd, is_pf = EXCLUDED.is_pf 
where EXCLUDED.score >= scores.score"""
            #cur.execute(query)
            counter += 1
            progress = f"{counter}/{len(beatmapIds)}"
            percentage = counter / len(beatmapIds)
            mycur.execute("UPDATE queue SET progress = ?, percentage = ? WHERE user_id = ?", (progress, percentage, userid))
            print(f'({progress}) found score for user {userid} on beatmap id: {beatmap_id}')
        else:
            counter += 1
            progress = f"{counter}/{len(beatmapIds)}"
            percentage = counter / len(beatmapIds)
            mycur.execute("UPDATE queue SET progress = ?, percentage = ? WHERE user_id = ?", (progress, percentage, userid))
            print(f'({progress}) no score found for user {userid} on beatmap id: {beatmap_id}')
    
    # when everything is done fetching
    mycur.execute("INSERT INTO fetched_users (user_id, username) VALUES (?, ?)", (userid, username))
    mycur.execute("DELETE FROM queue WHERE user_id = ?", (userid))
    print("All done.")

if validToken():
    print(userid)
    print(username)
    mycur.execute(f'select user_id as id from fetched_users where user_id={userid}')
    for (id) in mycur:
        if id[0] == userid:
            print('user already fetched.')
            sys.exit(1)
    mycur.execute(f'select user_id as id from queue where user_id={userid}')
    for (id) in mycur:
        if id[0] == userid:
            print('user already fetching.')
            sys.exit(1)

    print('user being fetched now.')
    progress = "Getting Beatmap IDs."
    mycur.execute('insert into queue (user_id, username, progress) values (?, ?, ?)', (userid, username, progress))
    getBeatmaps()
    fetchScores()

