# Purpose

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team needs insights as to what songs users are listening to. The raw data is not currently saved in a data format that is in a usable structure. The requirement is to translate the data into a structure that is easy to read and analyse. 

## Data Sources
The source data is recorded in two sets of JSON files sored in Amazon S3. The first set, the "Song" dataset contains information related to the song and artis. The second set of data, "log" contains data on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

1. The Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. Song data: s3://udacity-dend/song_data

2. Log dataset, generated from an event simulator related to songs from the Million Song dataset. These simulate app activity logs from a music streaming app based on specified configurations.Log data: s3://udacity-dend/log_data

The requiremet is to link the two data sets, and provide schemas that are easily queried by the analytics team for further inights.

# Running the application
The application is managed by running the cells sequentially as laid out in the Jupyter Notebook - datawarehouse.ipynb

# Design

The High level design is represented in the notebook comprising of the following steps and components:

## Star schema data repository
The database is designed following a Star schema, with a central fact table linked to referential dimensions.

The data is ingested pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.
directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Fact Table:

#### songplays
The sonplays table is made up of attributed derived from both the song, and log datasets.

Attributes: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

user_id and songid are derived from the songs dataset, by linking with the log attributes: song name, artist name and song length.

### Dimension Tables: 

Tthe dimesion tables are linked to the fact table by keys reperesented by attributes named with an 'xxx_id' suffix

#### users - users in the app
The users dimension is derived from the song dataset.
user_id (key), first_name, last_name, gender, level

#### songs - songs in music database
The songs dimension is derived from the song dataset.
song_id (key), title, artist_id, year, duration

#### artists - artists in music database
The artists dimension is derived from the song dataset.
Attributes: artist_id (key), name, location, lattitude, longitude

#### time - timestamps of records in songplays broken down into specific units
The time table is derived from the log dataset..
time_id (key), start_time, hour, day, week, month, year, weekday

## ETL Pipeline
The ETL pipeline facilitates the transfer of data from its raw state, to ists final prepared format. 

The ETL is broken down to the execution of follwing steps:
1. Extract from source files
2. Transform to required structures and data formats
3. Load into the Star schema structures

## Staging tables
2 staging tables are populated to source the Data warehouse. 
They are: 
1. staging_songs populated by s3://udacity-dend/song_data
2. staging_events populated by s3://udacity-dend/3://udacity-dend/log_data, Log data json path: s3://udacity-dend/log_json_path.json

## Distribution style
Distribution style is "AUTO" as the tables are relitavely small

## Sorting Keys
Tables are sorted by the keys that represent the primary point of interest, reflected by the corresponding value in the Fact table: songplays

# Manifest
the application is made up of the following objects, and are run in order:

1. datawarehouse.ipynb: Control Notebook 
2. create_tables.py: Generate data objects
3. sql.queries.py: sql managing data manipulation
4. etl.py: extract and load of log and song data
5. data_quality.ipynb: Data health and quality report
6. README.md