import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
    event_id INT IDENTITY(0,1),
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
    song_id VARCHAR,
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INT
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(
    user_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);

""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration NUMERIC
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    format as JSON {}
    """).format(config.get('S3', 'LOG_DATA'),
                   config.get('IAM_ROLE', 'ARN'),
                   config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    JSON 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
               config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
                
songplay_table_insert = ("""INSERT INTO songplays
                            (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                            )
                            SELECT DISTINCT timestamp 'epoch' + e.ts/1000 *
                                    interval '1 second' AS start_time,
                                e.userId,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.sessionId,
                                e.location,
                                e.userAgent
                            FROM staging_events e
                            JOIN staging_songs s
                            ON (e.artist = s.artist_name
                            AND e.song = s.title
                            AND e.length = s.duration)
                            WHERE e.page='NextSong'
                            ;""")
                
user_table_insert = ("""INSERT INTO users
                                    (user_id,
                                    first_name,
                                    last_name,
                                    gender,
                                    level)
                        SELECT DISTINCT userId, 
                                    firstName,
                                    lastName,
                                    gender,
                                    level
                        FROM staging_events
                        WHERE userId IS NOT NULL
                        ;""")

song_table_insert = ("""INSERT INTO songs
                                    (song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration)
                        SELECT DISTINCT song_id, 
                                    title,
                                    artist_id,
                                    year,
                                    duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
                        ;""")

artist_table_insert = ("""INSERT INTO artists
                                    (artist_id,
                                    name,
                                    location,
                                    latitude,
                                    longitude)
                          SELECT DISTINCT artist_id, 
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
                          ;""")

time_table_insert = ("""INSERT INTO time
                                    (start_time, 
                                    hour,
                                    day,
                                    week,
                                    month,
                                    year)
                        SELECT DISTINCT start_time, 
                            extract(h from start_time) as hour, 
                            extract(d from start_time) as day,
                            extract(w from start_time) as week,
                            extract(mon from start_time) as month,
                            extract(y from start_time) as year
                        FROM (SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time
                                FROM staging_events e)
                        ;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
