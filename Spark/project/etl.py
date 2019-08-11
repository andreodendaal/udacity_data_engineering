import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, dayofyear, \
    dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This function creates the SparkSession object
    and returns it as a variablefor reusable reference by the functions in this application 

    Input Parameters:
        None

    Returns:
        spark: instance object to access its public methods and instances for the duration the Spark job    
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.6,com.amazonaws:aws-java-sdk:1.7.4,net.java.dev.jets3t:jets3t:0.9.4") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads song data from s3 and creates:
        - songs table
        - artists table
        and writes the resulting table to S3

    Input Parameters:
        spark: instance object to access Spark public methods
        input_data: Root input data path in S3
        output_data: S3 Root output data path  

    Returns:
        song_stage_df: Data Frame holding the staged songs data read from S3. To be used to join with other data sets    
    """

    print('processing song data....')

    # get filepath to song data file    
    # songPath = "song_data/A/B/C/TRABCEI128F424C983.json"
    songPath = "song_data/*/*/*/*.json"
    input_data = input_data + songPath

    # read song data file
    song_stage_df = spark.read.json(input_data) \
        .withColumn("artist_id", col("artist_id").cast("int")) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("artist_latitude", col("artist_latitude").cast("float")) \
        .withColumn("artist_longitude", col("artist_latitude").cast("float"))

    # extract columns to create songs table
    songs_df = song_stage_df.selectExpr("song_id", "title", "artist_id", "cast(year as int) as year",
                                        "cast(duration as int) as duration") \
        .distinct()

    # write songs table to parquet files partitioned by year and artist     
    print('Writing "songs_table"....')

    songs_df.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .format("parquet") \
        .save(os.path.join(output_data, 'songs_table'))

    print('"songs_table" completed....')

    # extract columns to create artists table    
    print('processing artist data....')

    artists_df = song_stage_df.selectExpr("artist_id", "artist_name", "artist_location", "artist_latitude",
                                          "artist_longitude") \
        .distinct()

    # write artists table to parquet files
    print('Writing "artist_table"....')

    artists_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(os.path.join(output_data, 'artists_table'))

    print('"artist_table" completed....')

    print('**** All Song data has been processed ***')

    return song_stage_df


def process_log_data(spark, input_data, output_data):
    """
    This function reads log data from s3 and creates:
        - users_table
        - time table
        and writes the resulting table to S3

    Input Parameters:
        spark: instance object to access Spark public methods
        input_data: Root input data path in S3
        output_data: S3 Root output data path  

    Returns:
        log_stage_df: Data Frame holding the staged songs data read from S3. To be used to join with other data sets  

    """

    print('\noutput data path - {}'.format(output_data))

    print('\nprocessing log data....')
    # get filepath to log data file

    # log_data = 'log_data/2018/11/2018-11-12-events.json'    
    log_data = 'log_data/*/*/*.json'

    input_data = input_data + log_data

    # read log data file
    log_stage_raw_df = spark.read.json(input_data) \
        .withColumn("ts_str", col("ts").cast("string"))

    # filter by actions for song plays page=NextSong  
    log_stage_df = log_stage_raw_df.select("*").filter("page == 'NextSong'")

    print('processing user data....')
    # extract columns for users table    
    users_df = log_stage_df.selectExpr("cast(userId as int) as userId", "firstName", "lastName", "gender", "level") \
        .distinct()

    # write users table to parquet files
    print('Writing "user_table"....')

    users_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(os.path.join(output_data, 'users_table'))

    print('"users_table" completed....')

    print('processing time data....')
    # create timestamp column from original timestamp column
    # extract columns to create time table
    time_df = log_stage_df \
        .withColumn('dateColumn',
                    to_timestamp((log_stage_df['ts_str'] / 1000).cast('timestamp'), "yyyy-MM-dd hh:mm:ss")) \
        .select('dateColumn', date_format('dateColumn', 'h:m:s a').alias('dt_starttime'), \
                hour('dateColumn').alias('dt_hour'), \
                dayofyear('dateColumn').alias('dt_day'), \
                dayofweek('dateColumn').alias('dt_dayofweek'), \
                month('dateColumn').alias('dt_month'), year('dateColumn').alias('dt_year'))

    # time_df.printSchema()

    # write time table to parquet files partitioned by year and month
    print('Writing "time_table"....')

    # time_table
    time_df.write \
        .partitionBy("dt_year", "dt_month") \
        .mode("overwrite") \
        .format("parquet") \
        .save(os.path.join(output_data, 'time_table'))

    print('"time_table" completed....')

    print('*** All Log data has been processed ***')

    return log_stage_df


def process_song_plays(spark, output_data, song_stage_df, log_stage_df):
    """
    This function joins two datasets representing song and log data on:
        artist and song attributes, 
        and writes the result to S3
    Input Parameters:
        spark: instance object to access Spark public methods
        output_data: S3 Root output data path  
        song_stage_df
        log_stage_df

    Returns:
        Nothing

    """

    # read in song data to use for songplays table

    print('\njoining songplays data....')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = song_stage_df.join(log_stage_df, (log_stage_df.artist == song_stage_df.artist_name) & (
                log_stage_df.song == song_stage_df.title)) \
        .withColumn('dateColumn',
                    to_timestamp((log_stage_df['ts_str'] / 1000).cast('timestamp'), "yyyy-MM-dd hh:mm:ss")) \
        .select(log_stage_df["ts"], log_stage_df["userId"], log_stage_df["level"], song_stage_df["song_id"],
                song_stage_df["artist_id"], log_stage_df["sessionid"] \
                , log_stage_df["location"], log_stage_df["userAgent"], month('dateColumn').alias('dt_month'),
                year('dateColumn').alias('dt_year'))

    # write songplays table to parquet files partitioned by year and month        
    # songplays_table
    print('Writing "songplays_table"....')

    songplays_df.write \
        .partitionBy("dt_year", "dt_month") \
        .mode("overwrite") \
        .format("parquet") \
        .save(os.path.join(output_data, 'songplays_table'))

    print('"songplays_table" completed....')

    print('*** !All data has been processed! ***')


def main():
    spark = create_spark_session()
    print('spark sesssion - {}'.format(spark))

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aod-datalake-bucket/"

    songfile = process_song_data(spark, input_data, output_data)
    logfile = process_log_data(spark, input_data, output_data)
    process_song_plays(spark, output_data, songfile, logfile)


if __name__ == "__main__":
    main()
