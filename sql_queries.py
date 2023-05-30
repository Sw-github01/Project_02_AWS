import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_event (
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        item_in_session INTEGER,
        last_name TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page INTEGER,
        registration FLOAT,
        session_id CHAR(10),
        song TEXT,
        status INTEGER,
        ts TIMESTAMP,
        user_agent TEXT,
        user_id CHAR(10)
        );

""")

staging_songs_table_create = ("""
    CREATE TABLE staging_song (
        num_songs INTEGER,
        artist_id CHAR(10),
        artist_latitude TEXT,
        artist_longitude TEXT,
        artist_location TEXT,
        artist_name TEXT,
        song_id CHAR(10),
        title TEXT,
        duration FLOAT,
        year TEXT
);
""")

songplay_table_create = ("""
    CREATE TABLE songplay_table (
        songplay_id CHAR(10) PRIMARY KEY SORTKEY,
        start_time TIMESTAMP,
        user_id CHAR(10),
        level CHAR(10),
        song_id CHAR(10),
        artist_id CHAR(10),
        session_id CHAR(10),
        location TEXT,
        user_agent TEXT
);
""")

user_table_create = ("""
    CREATE TABLE user_table (
        user_id CHAR(10) PRIMARY KEY SORTKEY,
        firstname TEXT,
        lastname TEXT,
        gender TEXT,
        level TEXT
);
""")

song_table_create = ("""
    CREATE TABLE song_table (
        song_id CHAR(10) PRIMARY KEY SORTKEY,
        title TEXT,
        artist_id CHAR(10),
        year INTEGER,
        duration FLOAT
);
""")

artist_table_create = ("""
    CREATE TABLE artist_table (
        artist_id CHAR(10) PRIMARY KEY SORTKEY,
        name TEXT,
        location TEXT,
        lattitude CHAR(10),
        longitude CHAR(10)
);
""")

time_table_create = ("""
    CREATE TABLE time_table (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INTEGER,
        day TEXT,
        week TEXT,
        month TEXT,
        year INTEGER,
        weekday TEXT
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_event from {}
credentials 'aws_iam_role={}'
json {};

""").format(config.get("S3", "LOG_DATA"),
config.get("IAM_ROLE", "ARN"),
config.get("S3", "LOG_JSONPATH"))


staging_songs_copy = ("""
copy staging_song from {}
credentials 'aws_iam_role={}'
json 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT 
    TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'),
    se.user_id,
    se.level,
    so.song_id,
    so.artist_id,
    se.session_id,
    se.location,
    se.user_agent
 FROM staging_event se
 LEFT JOIN staging_song so ON
    se.song = so.title AND
    se.artist = so.artist_name AND
    ABS(se.length - so.duration) <2
 WHERE
    se.page = 'NextSong'   
""")

user_table_insert = ("""
INSERT INTO user_table 
SELECT DISTINCT (user_id)
    user_id,
    first_name,
    last_name,
    gender,
    level
    FROM staging_event
""")

song_table_insert = ("""
INSERT INTO song_table 
SELECT DISTINCT (song_id)
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_song
""")

artist_table_insert = ("""
INSERT INTO artist_table 
SELECT DISTINCT (artist_id)
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_song
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
