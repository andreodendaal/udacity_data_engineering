# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS songplays;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id varchar, start_time timestamp, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar, user_agent varchar, PRIMARY KEY (songplay_id));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar, PRIMARY KEY (user_id));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar, title varchar, artist_id varchar, year int, duration int, PRIMARY KEY (song_id));
""")
#song_id	title	artist_id	year	duration

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar, location varchar, lattitude float, longitude float, PRIMARY KEY (artist_id));
""")

# https://www.postgresql.org/docs/9.1/functions-datetime.html
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int, PRIMARY KEY (start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agen) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s ) ON CONFLICT (user_id) DO NOTHING;""")

song_table_insert =  ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s);""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) VALUES(%s, %s, %s, %s, %s);""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s);""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

#create_table_queries = [songplay_table_create]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#drop_table_queries = [songplay_table_drop]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]