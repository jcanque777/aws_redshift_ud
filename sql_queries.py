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
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT NOT NULL,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration NUMERIC,
    sessionId INT NOT NULL,
    song VARCHAR,
    status INT NOT NULL,
    ts NUMERIC NOT NULL,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
    song_id VARCHAR PRIMARY KEY,
    num_songs INT NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude VARCHAR,
    artist_longitude NUMERIC,
    artist_location NUMERIC,
    artist_name VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration NUMERIC NOT NULL,
    year INT NOT NULL
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
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
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);

""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration NUMERIC NOT NULL
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    JSON '{}'
    """).format(config.get('S3', 'LOG_DATA'),
               (config.get('IAM_ROLE', 'ARN'),
               (config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    JSON 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
               config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES


                
                
                
                
songplay_table_insert = ("""INSERT INTO songplay 
                            (
                            songplay_id,
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                            )
                            SELECT DISTINCT s.song_id,
                                e.ts,
                                e.userId,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.sessionId,
                                e.location,
                                u.userAgent
                            FROM staging_events e, staging_songs s
                            WHERE e.page='NextSong' AND e.song=s.title
                            """)

  """
    e(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT NOT NULL,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration NUMERIC,
    sessionId INT NOT NULL,
    song VARCHAR,
    status INT NOT NULL,
    ts NUMERIC NOT NULL,
    userAgent VARCHAR,
    userId INT

    S
    song_id VARCHAR PRIMARY KEY,
    num_songs INT NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude VARCHAR,
    artist_longitude NUMERIC,
    artist_location NUMERIC,
    artist_name VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration NUMERIC NOT NULL,
    year INT NOT NULL"""
                
user_table_insert = ("""INSERT INTO users 
                                    (user_id, first_name, last_name, gender,level)
                        SELECT DISTICT userId, firstName, lastName, gender, level
                        FROM staging_events
                        WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs
                                    (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists
                                    (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time
                                    (start_time, hour,day, week, month, year, weekday)
                        SELECT DISTINCT ts,
                                        extract (hour from ts),
                                        extract (day from ts),
                                        extract (week from ts),
                                        extract (month from ts),
                                        extract (year from ts),
                                        extract (weekday from ts)
                        FROM staging_events                                    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
