import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
users_staging_drop = "DROP TABLE IF EXISTS users_staging"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
artist varchar,
auth varchar(25),
firstName varchar,
gender varchar(10),
itemInSession integer,
lastName varchar(25),
length decimal,
level varchar(10),
location varchar,
method varchar(4),
page varchar(25),
registration decimal,
sessionId integer,
song varchar,
status integer,
ts BIGINT ,
userAgent varchar,
userId integer
)

""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
num_songs integer,
artist_id varchar(50), 
artist_latitude decimal,
artist_longitude decimal, 
artist_location varchar, 
artist_name varchar, 
song_id varchar(50), 
title varchar, 
duration decimal, 
year integer
)

""")

songplay_table_create = ("""CREATE TABLE songplays (
songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
start_time timestamp NOT NULL,
user_id integer NOT NULL, 
level varchar(25),  
artist_id varchar(50),
session_id integer, 
song_id varchar(50) sortkey distkey, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""CREATE TABLE users (
user_id int PRIMARY KEY sortkey,
first_name varchar(25), 
last_name varchar(25), 
gender varchar(10), 
level varchar(10),
ts BIGINT 
)
diststyle all
""")

user_staging_create = ("""CREATE TABLE users_staging (
user_id int PRIMARY KEY sortkey,
first_name varchar(25), 
last_name varchar(25), 
gender varchar(10), 
level varchar(10),
ts BIGINT 
)
"""
)

song_table_create = ("""CREATE TABLE songs (
song_id varchar PRIMARY KEY sortkey distkey,
title varchar NOT NULL,
artist_id varchar(50),
year int NOT NULL, 
duration float NOT NULL
)

""")

artist_table_create = ("""CREATE TABLE artists (
artist_id varchar PRIMARY KEY sortkey, 
name varchar NOT NULL,
location varchar, 
latitude decimal, 
longitude decimal
)
""")

time_table_create = ("""CREATE TABLE time (
start_time timestamp PRIMARY KEY sortkey,
hour integer,
day integer, 
week integer,
month integer,
year integer, 
weekday integer
)
diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
credentials 'aws_iam_role={}'
json {} region 'us-west-2';
        """.format(config['S3']['LOG_DATA'], config["IAM_ROLE"]['ARN'], config['S3']['LOG_JSONPATH'])
                      )
staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto' region 'us-west-2';
        """.format(config['S3']['SONG_DATA'], config["IAM_ROLE"]['ARN'])
                     )
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, userId, level, song_id, artist_id,
sessionId, location, userAgent                    
FROM staging_songs s 
JOIN staging_events e
ON  s.title = e.song AND s.artist_name = e.artist
WHERE e.page='NextSong' 

""")
#using staging table(users_staging) with the same values and use it to remove the old level of the user and keep just 
#the new one without any duplicats 
user_table_insert = ("""INSERT INTO users(
user_id ,
first_name , 
last_name , 
gender , 
level,
ts )
SELECT userId, firstName, lastName, gender, level, ts
FROM staging_events
WHERE userId IS NOT NULL;

INSERT INTO users_staging(
user_id ,
first_name , 
last_name , 
gender , 
level,
ts )
SELECT userId, firstName, lastName, gender, level, ts
FROM staging_events
WHERE userId IS NOT NULL;

DELETE FROM users
USING users_staging
WHERE users.user_id = users_staging.user_id
AND users_staging.ts > users.ts;

DROP TABLE IF EXISTS users_staging;
""")

song_table_insert = ("""INSERT INTO songs(
song_id ,
title,
artist_id,
year, 
duration
)
SELECT 
song_id ,title , artist_id , year, duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists(
artist_id, 
name,
location, 
latitude, 
longitude
)
SELECT 
artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
start_time,
EXTRACT (HOUR FROM start_time), 
EXTRACT (DAY FROM start_time),
EXTRACT (WEEK FROM start_time), 
EXTRACT (MONTH FROM start_time),
EXTRACT (YEAR FROM start_time), 
EXTRACT (WEEKDAY FROM start_time) 
FROM
(
SELECT  TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
FROM staging_events
)

""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, user_staging_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, users_staging_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
