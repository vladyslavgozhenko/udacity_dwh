import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS se_staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS ss_staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS sp_songplays"
user_table_drop = "DROP TABLE IF EXISTS u_users"
song_table_drop = "DROP TABLE IF EXISTS s_songs"
artist_table_drop = "DROP TABLE IF EXISTS a_artists"
time_table_drop = "DROP TABLE IF EXISTS t_times"

# CREATE TABLES



staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS ss_staging_songs (
                                    ss_artist_id VARCHAR,
                                    ss_latitude FLOAT,
                                    ss_location VARCHAR, 
                                    ss_longitude FLOAT,
                                    ss_name VARCHAR,
                                    ss_duration FLOAT,    
                                    ss_num_songs VARCHAR,
                                    ss_song_id VARCHAR,
                                    ss_title VARCHAR,
                                    ss_year INTEGER
                                    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS  se_staging_events (
                                    se_artist_id VARCHAR, 
                                    se_auth VARCHAR, 
                                    se_first_name VARCHAR, 
                                    se_gender VARCHAR, 
                                    se_item_in_Session  INTEGER, 
                                    se_last_name VARCHAR, 
                                    se_duration FLOAT, 
                                    se_level VARCHAR, 
                                    se_location VARCHAR, 
                                    se_method VARCHAR, 
                                    se_page VARCHAR, 
                                    se_registration VARCHAR, 
                                    se_session_id  INTEGER,
                                    se_title VARCHAR,
                                    se_status INTEGER,
                                    se_timestamp TIMESTAMP,
                                    se_user_agent VARCHAR,
                                    se_user_id INTEGER
                                    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS sp_songplays (
                                    sp_songplay_id VARCHAR(22),
                                    sp_start_time TIMESTAMPTZ NOT NULL,
                                    sp_user_id INTEGER  NOT NULL sortkey,
                                    sp_level VARCHAR(22) NOT NULL,
                                    sp_song_id VARCHAR(22) NOT NULL,
                                    sp_artist_id VARCHAR(30) NOT NULL,
                                    sp_session_id INTEGER NOT NULL,
                                    sp_location VARCHAR(50) NOT NULL,
                                    sp_user_agent VARCHAR(22) NOT NULL,
                                    PRIMARY KEY(sp_start_time, sp_user_id),
                                    FOREIGN KEY(sp_artist_id) references a_artists(a_artist_id),
                                    FOREIGN KEY(sp_user_id) references u_users(u_user_id),
                                    FOREIGN KEY(sp_song_id) references s_songs(s_song_id),
                                    FOREIGN KEY(sp_start_time) references t_time(t_start_time)
                                    )
                                    diststyle all;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS u_users (
                                    u_user_id INTEGER NOT NULL PRIMARY KEY sortkey,
                                    u_first_name VARCHAR(22) NOT NULL,
                                    u_last_name VARCHAR(22) NOT NULL,
                                    u_gender CHAR NOT NULL,
                                    u_level VARCHAR(22) NOT NULL
                                    )
                                    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS s_songs (
                                    s_song_id VARCHAR(22) NOT NULL PRIMARY KEY sortkey,
                                    s_title VARCHAR(26) NOT NULL,
                                    s_artist_id VARCHAR(30) NOT NULL,
                                    s_year INTEGER NOT NULL,
                                    s_duration FLOAT NOT NULL
                                    )
                                    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS a_artists (
                                    a_artist_id VARCHAR(30) NOT NULL PRIMARY KEY sortkey,
                                    a_name VARCHAR(22) NOT NULL,
                                    a_location VARCHAR(50) NOT NULL,
                                    a_latitude FLOAT,
                                    a_longitude FLOAT
                                    )
                                    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS t_time (
                                t_start_time TIMESTAMPTZ NOT NULL PRIMARY KEY sortkey,
                                t_hour INTEGER NOT NULL, 
                                t_day INTEGER NOT NULL,
                                t_week INTEGER NOT NULL,
                                t_month INTEGER NOT NULL,
                                t_year INTEGER NOT NULL,
                                t_weekday INTEGER NOT NULL)
                                diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy se_staging_events from '{}'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS  EMPTYASNULL BLANKSASNULL 
    COMPUPDATE OFF region 'us-west-2';
    """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['DWH_IAM_ROLE'])

staging_songs_copy = ("""
    COPY ss_staging_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['DWH_IAM_ROLE'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO sp_songplays (sp_song_id, sp_user_id, sp_level,
                        sp_start_time, sp_artist_id, sp_session_id,
                        sp_location, sp_user_agent)
select 
            ss.ss_song_id as sp_song_id,
            se.se_user_id as sp_user_id,
            se.se_level as sp_level,
            se.se_timestamp as sp_start_time,
            se.se_artist_id as sp_artist_id,
            se.se_session_id as sp_session_id,
            se.se_location as sp_location,
            se.se_user_agent as sp_user_agent

FROM se_staging_events as se, ss_staging_songs as ss
WHERE
        se.se_artist_id=ss.ss_artist_id AND
        se.se_title= ss.ss_title AND
        se.se_duration = ss.ss_duration;
""")

user_table_insert = ("""
INSERT INTO u_users (u_user_id, 
                     u_first_name,
                     u_last_name,
                     u_gender,
                     u_level)
SELECT se_user_id as u_user_id, 
       se_first_name as u_first_name, 
       se_last_name as u_last_name, 
       se_gender as u_gender,
       se_level as u_level
FROM se_staging_events
WHERE se_user_id IS NOT NULL AND
       se_first_name IS NOT NULL AND 
       se_last_name IS NOT NULL AND 
       se_gender IS NOT NULL AND
       se_level IS NOT NULL;

""")
# ON CONFLICT u_user_id DO NOTHING
# UPDATE
# SET
#     u_first_name = se_first_name,
#     u_last_name = se_last_name,
#     u_gender = se_gender,
#     u_level = se_level

song_table_insert = ("""
INSERT INTO s_songs (s_song_id,
                     s_title,
                     s_artist_id,
                     s_year,
                     s_duration)
SELECT ss_song_id as s_song_id, 
        ss_title as s_title, 
        ss_artist_id as s_artist_id,
        ss_year as s_year,
        ss_duration as s_duration
FROM ss_staging_songs
WHERE ss_song_id IS NOT NULL AND 
        ss_title IS NOT NULL AND  
        ss_artist_id IS NOT NULL AND 
        ss_year IS NOT NULL AND 
        ss_duration IS NOT NULL;
""")
# ON CONFLICT (s_song_id) DO NOTHING
# UPDATE 
# SET
#     s_title = ss_title,
#     s_artist_id = ss_artist_id,
#     s_year = ss_year,
#     s_duration = ss_duration

artist_table_insert = ("""
INSERT INTO a_artists (a_artist_id, 
                       a_name,
                       a_location,
                       a_latitude,
                       a_longitude)
SELECT ss_artist_id as a_artist_id, 
       ss_name as a_name, 
       ss_location as a_location,
       ss_latitude as a_latitude,
       ss_longitude as a_longitude
from ss_staging_songs
WHERE ss_artist_id IS NOT NULL;
""")
# ON CONFLICT (a_artist_id) DO NOTHING
# UPDATE
# SET
#         a_artist_id = ss_artist_id, 
#         a_name  = ss_name,
#         a_location  = ss_location,
#         a_latitude  = ss_latitude,
#         a_longitude = ss_longitude

time_table_insert = ("""
INSERT INTO t_time (t_start_time,
                  t_hour,
                  t_day,
                  t_week,
                  t_month,
                  t_year,
                  t_weekday) 
SELECT se_timestamp as t_start_time,
EXTRACT(hour from se_timestamp) as t_hour,
EXTRACT(day from se_timestamp) as t_day,
EXTRACT(week from se_timestamp) as t_week,
EXTRACT(month from se_timestamp) as t_month,
EXTRACT(year from se_timestamp) as t_year,
EXTRACT(dow from se_timestamp) as t_weekday
FROM se_staging_events
WHERE se_timestamp IS NOT NULL;    
""")
#ON CONFLICT (start_time) DO NOTHING

# delete_null_rows_staging_songs = ("""
# DELETE FROM ss_staging_songs WHERE
#                                     ss_artist_id  IS NULL OR
#                                     ss_location  IS NULL  OR
#                                     ss_name IS NULL  OR
#                                     ss_duration  IS NULL  OR    
#                                     ss_num_songs  IS NULL OR
#                                     ss_song_id  IS NULL OR
#                                     ss_title IS NULL OR
#                                     ss_year  IS NULL
# """
# )

# delete_null_rows_staging_events = ("""
# DELETE FROM ss_staging_events WHERE
#                                     se_artist_id   IS NULL OR 
#                                     se_auth   IS NULL OR 
#                                     se_first_name   IS NULL OR
#                                     se_gender CHAR   IS NULL OR 
#                                     se_item_in_Session   IS NULL OR 
#                                     se_last_name   IS NULL OR
#                                     se_duration IS NULL OR 
#                                     se_level   IS NULL OR
#                                     se_location   IS NULL OR
#                                     se_method  IS NULL OR
#                                     se_page  IS NULL OR
#                                     se_registration  IS NULL OR 
#                                     se_session_id  IS NULL OR
#                                     se_title   IS NULL OR
#                                     se_status   IS NULL OR
#                                     se_timestamp  IS NULL OR
#                                     se_user_agent  IS NULL OR
#                                     se_user_id   IS NULL
# """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
#del_rows_with_null = [delete_null_rows_staging_songs, delete_null_rows_staging_events]