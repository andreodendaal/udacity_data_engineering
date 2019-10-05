COPY_SQL_SONGS = """
        COPY {0}
        FROM 's3://{1}/{2}'
        ACCESS_KEY_ID '{3}'
        SECRET_ACCESS_KEY '{4}'
        JSON 'auto'
        REGION '{5}'
        COMPUPDATE OFF;
        """ 

COPY_SQL_EVENTS = """
        COPY {0} 
        FROM 's3://{1}/{2}'
        ACCESS_KEY_ID '{3}'
        SECRET_ACCESS_KEY '{4}'
        REGION '{5}'
        FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
        """

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs 
        (
        num_songs     VARCHAR NOT NULL,
        artist_id       VARCHAR,
        artist_latitude        VARCHAR,
        artist_longitude   VARCHAR,
        artist_location      VARCHAR,
        artist_name       VARCHAR,
        song_id        VARCHAR,
        title        VARCHAR, 
        duration    VARCHAR,
        year       VARCHAR  
        );
        """)

staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events 
        (
        artist     VARCHAR,
        auth       VARCHAR,
        firstName        VARCHAR,
        gender   VARCHAR,
        iteminSession      VARCHAR,
        lastName       VARCHAR,
        length        VARCHAR,
        level        VARCHAR, 
        location    VARCHAR,
        method    VARCHAR,
        page    VARCHAR,
        registration    VARCHAR,
        sessionid    VARCHAR,
        song    VARCHAR,
        status    VARCHAR,
        ts    VARCHAR,
        userAgent    VARCHAR,
        userId       VARCHAR  
         );
        """)   

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(0,1) PRIMARY KEY, 
        start_time varchar, 
        user_id int, 
        song_id varchar, 
        artist_id varchar, 
        session_id int sortkey, 
        location varchar, 
        user_agent varchar,
        level varchar
        );
        """)
        
songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
        SELECT \
        TIMESTAMP WITH TIME ZONE 'epoch' + CAST(staging_events.ts AS NUMERIC) * INTERVAL '1 Second ', \
        CAST(staging_events.userId AS INT), \
        staging_events.level, \
        staging_songs.song_id, \
        staging_songs.artist_id, \
        CAST(staging_events.sessionid AS INT), \
        staging_events.location, \
        staging_events.userAgent \
        FROM staging_events join staging_songs on staging_events.artist = staging_songs.artist_name and staging_events.song = staging_songs.title
        """)

user_table_insert = ("""
        INSERT INTO users (user_id , first_name, last_name, gender, level) \
        SELECT DISTINCT CAST(userId AS INT), firstName, lastName, gender, level FROM staging_events where \
        userId != ' '; """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) SELECT song_id, title, artist_id, CAST(year AS INT), CAST(duration AS NUMERIC) FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) SELECT DISTINCT artist_id, artist_name, artist_location , \
                CAST(artist_latitude AS FLOAT),CAST(artist_longitude AS FLOAT) FROM staging_songs;""")

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
        SELECT \
        TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC) * INTERVAL '1 Second ' AS c_Time, \
        extract(hr from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC) * INTERVAL '1 Second ')) AS c_Hour, \
        extract(day from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC)  * INTERVAL '1 Second ')) AS c_Day, \
        extract(w from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC)  * INTERVAL '1 Second ')) AS c_Week, \
        extract(mon from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC) * INTERVAL '1 Second '))AS c_Month, \
        extract(y from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC) * INTERVAL '1 Second ')) AS c_Year, \
        extract(weekday from (TIMESTAMP WITH TIME ZONE 'epoch' + CAST(ts AS NUMERIC) * INTERVAL '1 Second ')) AS c_Weekday \
        FROM staging_events
        """)

user_table_create = ("""

        CREATE TABLE IF NOT EXISTS users (
        user_id int not null sortkey, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar 
        );
        """)

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
        song_id varchar not null sortkey, 
        title varchar, 
        artist_id varchar NOT NULL, 
        year int, 
        duration numeric NOT NULL 
        );
        """)

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar not null sortkey, 
        name varchar, 
        location varchar DEFAULT 'None', 
        lattitude float DEFAULT 0, 
        longitude float DEFAULT 0
        );
        """)

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
        time_id int identity(0,1) PRIMARY KEY , 
        start_time TIMESTAMP not null sortkey, 
        hour int,
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int 
        );
        """)  