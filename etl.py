# Load libraries
%load_ext sql
import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofweek, hour, weekofyear

# Access key and secret seed to access aws environment
# Note: THe information are only needed in case the script is run from non-aws environment
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS CREDENTIALS']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS CREDENTIALS']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
        """
    Description: The function builds and configurate the spark session. Does not need to be called in 
        case the code is executed on notebook running on pyspark kernel.
    Parameters:
        None
    Returns:
        spark: spark session e.g. needed to read json files.
    """
    spark = SparkSession \
        .builder \
        .appName("Sparkify Datalake") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description: The function collects the song data from the
        filepath in the S3 bucket from udacity to get the song and artist details and
        populate the songs and artists dimension tables.
    Parameters:
        spark: The cursor object containing the reference to spark session.
        input_path: The root path to the bucket containing song data.
        output_path: The path to the destination bucket where the parquet files
            will be stored.
    Returns:
        None
    """
    
    # Get filepath to data source
    song_data = "song_data/*/*/*/*.json"
    
    # Read song data files into respective dataframe schemas
    df_song_data = spark.read.json(input_data + song_data)
    
    ## Extract data and push them into dataframes
    # Extract songs columns and modify its datatype to create songs table
    df_songs_table = df_song_data.selectExpr("song_id", 
                                             "title", 
                                             "artist_id", 
                                             "cast(song_id as int) year", 
                                             "cast(song_id as float) duration")

    # Extract columns to create artists table
    df_artists_table = df_song_data.selectExpr("artist_id", 
                                               "artist_name", 
                                               "artist_location", 
                                               "cast(artist_latitude as float) artist_latitude", 
                                               "cast(artist_longitude as float) artist_longitude") \
                                               .dropDuplicates(["artist_id"])
    
    print("Success: Create artist and song dataframe")
    
    ## Save dataframes (tables) as parquet files
    
    # Write songs back to parquet files partitioned by year and artist id
    songs_table_parquet = df_songs_table.write.parquet(partitionBy=["year","artist_id"], 
                                                       path=output_data_root + "song_data/songs.parquet", 
                                                       mode="overwrite")

    # Write artists back to parquet file
    artists_table_parquet = df_artists_table.write.parquet(path=output_data_root + "song_data/artists.parquet", 
                                                           mode="overwrite")
    
    print("Success: Write data artist and song data into parquet files")

def process_log_data(spark, input_data, output_data):
    """
    Description: The function collects the log data from the
        filepath in the S3 bucket from udacity to get the user, songsplays and time details and
        populate the fact table songsplays as well the dimension tables user and time.
    Parameters:
        spark: The cursor object containing the reference to spark session.
        input_path: The root path to the bucket containing song data.
        output_path: The path to the destination bucket where the parquet files
            will be stored.
    Returns:
        None
    """

    # Get filepath to data source
    song_data = "song_data/*/*/*/*.json"
    log_data = "log_data/*/*/*.json"

    # Read log data files into respective dataframe schemas
    df_log_data = spark.read.json(input_data + log_data)
    df_song_data = spark.read.json(input_data + song_data)
    
    ## Extract data and push them into dataframes

    # Udf helper function
    # Extract datetime based on the timestamp from log dataframe
    get_datetime_udf = udf(lambda ts: datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    # Add timestamp column to log data frame as it is needed by songsplay and time dateframe tables
    df_log_data = df_log_data.withColumn("datetime", get_datetime_udf(df_log_data.ts))

    # Create Fact table containing all played songs
    # Combination of songs and artist describing available item
    # Items which have been actually played are described by calling NextSong page
    df_songsplay_table = df_log_data.join(df_song_data, (df_log_data.artist == df_song_data.artist_name) & \
                                        (df_log_data.song == df_song_data.title)) \
                                            .select("userId",
                                                    "song_Id",
                                                    "artist_Id",
                                                    "sessionId",
                                                    "level",
                                                    "location",
                                                    "userAgent",
                                                    "datetime",
                                                    month("datetime").alias("month"),
                                                    year("datetime").alias("year"),) \
                                            .filter(df_log_data.page == "NextSong")
    
    # Extract user columns to create users table
    df_users_table = df_log_data.select("userId", 
                                        "firstName", 
                                        "lastName", 
                                        "gender", 
                                        "level") \
                                        .dropDuplicates(["userId"])

    # Create time table dataframe for all songs which have been played
    df_time_table = df_songsplay_table.select([hour("datetime").alias("hour"),
                                              month("datetime").alias("month"),
                                              year("datetime").alias("year"),
                                              dayofweek("datetime").alias("weekday"),
                                              weekofyear("datetime").alias("weekofyear"),
                                              "datetime"])
    
    print("Success: Create user, songsplay and time dataframe")

    ## Save dataframes (tables) as parquet files
    
    # Write users back to parquet file
    users_table_parquet = df_users_table.write.parquet(path=output_data_root + "user_data/users.parquet", 
                                                       mode="overwrite")

     # Write played songs back to parquet file partitioned by year and month
    songplays_table_parquet = df_songsplay_table.write.parquet(partitionBy=["year","month"], 
                                                               path=output_data_root + "songsplay_data/songsplay.parquet", 
                                                               mode="overwrite")

    # Write time details back to parquet file partitioned by year and month
    time_table_parquet = df_time_table.write.parquet(partitionBy=["year","month"], 
                                                     path=output_data_root + "time_data/time.parquet", 
                                                     mode="overwrite")
    
    print("Success: Write data user, songsplay and time data into parquet files")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3-bucket-udacity/data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    
if __name__ == "__main__":
    main()