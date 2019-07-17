import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# The table names are stored in variables to make modifications of table names in queries simplier and unified
staging_songs = "staging_songs"
staging_events = "staging_events"
song_plays = "songplays"
users = "users"
songs = "songs"
artists = "artists"
time = "time"

# DROP TABLES

# The beginning part of all the drop queries are the same, so a variable is used to store and reuse that
drop_starter = "DROP TABLE IF EXISTS "

# The drop queries are constructed using the drop starter variable along with the table names
staging_events_table_drop = drop_starter + staging_events
staging_songs_table_drop = drop_starter + staging_songs
songplay_table_drop = drop_starter + song_plays
user_table_drop = drop_starter + users
song_table_drop = drop_starter + songs
artist_table_drop = drop_starter + artists
time_table_drop = drop_starter + time

# CREATE TABLES

# The beginning part of all the create queries are the same, so a variable is used to store and reuse that
create_starter = "CREATE TABLE IF NOT EXISTS "

# The create queries are created using the create starter variable, the table name, and the list of column definitions
staging_events_table_create = create_starter + staging_events + ("""
        (
            artist            varchar,
            auth              varchar,
            firstName         varchar,
            gender            varchar,
            itemInSession     int,
            lastName          varchar,
            length            float,
            level             varchar,
            location          varchar,
            method            varchar,
            page              varchar,
            registration      float,
            sessionId         int,
            song              varchar,
            status            int,
            ts                bigint,
            userAgent         varchar,
            userId            varchar
        );
    """)

staging_songs_table_create = create_starter + staging_songs + ("""
        (
            num_songs         int,
            artist_id         varchar,
            artist_latitude   float,
            artist_longitude  float,
            artist_location   varchar,
            artist_name       varchar,
            song_id           varchar,
            title             varchar,
            duration          float,
            year              int
        );
    """)

songplay_table_create = create_starter + song_plays + (""" 
        (
            songplay_id   int       identity(0,1)    primary key, 
            start_time    bigint    not null, 
            user_id       varchar   not null, 
            level         varchar, 
            song_id       varchar   not null, 
            artist_id     varchar   not null, 
            session_id    int, 
            location      varchar, 
            user_agent    varchar
        );
    """) 

user_table_create = create_starter + users + (""" 
        (
            user_id       varchar   primary key, 
            first_name    varchar, 
            last_name     varchar, 
            gender        varchar, 
            level         varchar
        );
    """)
song_table_create = create_starter + songs + (""" 
        (
            song_id       varchar   primary key, 
            title         varchar, 
            artist_id     varchar, 
            year          int, 
            duration      float
        );
    """)
artist_table_create = create_starter + artists + (""" 
        (
            artist_id     varchar   primary key, 
            name          varchar, 
            location      varchar, 
            latitude      float, 
            longitude     float
        );
    """)
time_table_create = create_starter + time + (""" 
        (
            start_time    bigint    primary key, 
            hour          int, 
            day           int, 
            week          int, 
            month         int, 
            year          int, 
            weekday       varchar
        );
    """)

# STAGING TABLES

#The copy commands used to stage the song and event files into the staging tables.
staging_events_copy = ("""
    copy {} from {} 
    credentials 'aws_iam_role={}' 
    JSON {} 
    compupdate off region 'us-west-2';
""").format(staging_events, config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy {} from {} 
    credentials 'aws_iam_role={}' 
    JSON 'auto'
    compupdate off region 'us-west-2';
""").format(staging_songs, config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

# The beginning and middle part of all the insert queries are the same, so variables are used to store and reuse those
insert_starter = "INSERT INTO "

# The insert queries are created using the insert starter variable, the table name, and the select query used to generate the data
songplay_table_insert = insert_starter + song_plays + " " + ("""
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select 
        e.ts as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
    from staging_events e
    left join staging_songs s on e.song = s.title and e.artist = s.artist_name
    where e.page = 'NextSong'
""")

user_table_insert = insert_starter + users + " " + ("""
    select 
        distinct userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    from staging_events
""") 

song_table_insert = insert_starter + songs + " " + ("""
    select 
        distinct song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
""")

artist_table_insert = insert_starter + artists + " " + ("""
    select 
        distinct artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs
""")

time_table_insert = insert_starter + time + " " + ("""
    select 
        ts as start_time,
        EXTRACT(hrs FROM ts_date) as hour,
        EXTRACT(d FROM ts_date) as day,
        EXTRACT(w FROM ts_date) as week,
        EXTRACT(mon FROM ts_date) as month,
        EXTRACT(yr FROM ts_date) as year, 
        EXTRACT(dow FROM ts_date) as weekday
    from (select distinct ts,'1970-01-01 00:00:00'::timestamp + ts/1000 * interval '1 second' as ts_date from staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
