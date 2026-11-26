from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,expr,count, sum as _sum, window,avg,approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import time
import os

def clear_screen():
    # Clear the console screen based on the operating system
    os.system('clear' if os.name == 'posix' else 'cls')


print("=`"*80  )
print("Starting Spark Structured Streaming application to read from Kafka topic 'music-streaming-events'...\n")
print("="*80  )
print("Powered by spark memory sink - Interactive query mode\n")
print("="*80  )

#initialize Spark avec unique checkpoint location
spark = SparkSession.builder \
    .appName("MusicStreamingDashboard") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()


print("Spark Session initialized successfully.\n")


#schema for the incoming JSON data from Kafka
schema = StructType([
    StructField("user_id", StringType()),
    StructField("track_title", StringType()),
    StructField("artist_name", StringType()),
    StructField("album_name", StringType()),
    StructField("genre", StringType()),
    StructField("action_type", StringType()),
    StructField("action_description", StringType()),
    StructField("play_duration", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("device", StringType()),
    StructField("location", StringType()),
    StructField("user_tier", StringType())
])  

#read from kafka

print("Reading from Kafka topic 'music-streaming-events'...\n")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music-streaming-events") \
    .option("startingOffsets", "latest") \
    .load()

print("kafka stream read successfully. Schema of raw stream:")

raw_stream.printSchema()

#parse the JSON data from the Kafka stream
events_df = raw_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", expr("timestamp_millis(timestamp)")) \
    .withWatermark("event_time", "10 seconds")  # Set watermark for late data handling



print("data pipeline ready. Schema of parsed stream:")
events_df.printSchema()

print("="*80  )
print(" Creating real-time music dashboard with the following metrics:\n")
print("="*80)

#Dashboard 1: Raw event (for debugging)

print("Dashboard 1: Raw events (for debugging) - \n")

activity_query = events_df.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("streaming_activity") \
    .option("checkpointLocation","/tmp/spark_checkpoint/activity") \
    .start()

#lists Dashboard 2: top art (like wrapped) - simplified version
print("Dashboard 2: Top Artists (like Wrapped) - \n")
top_artists_query = events_df \
    .filter(col("action_type") == "play") \
    .groupBy("artist_name") \
    .agg(
        count("*").alias("play_count"),
        _sum("play_duration").alias("total_play_duration_ms"),
        approx_count_distinct("user_id", 0.05).alias("unique_listeners_approx")
    ) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("top_artists") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation","/tmp/spark_checkpoint/top_artists") \
    .start()

#Dashboard 3: Top Tracks (like Wrapped) - simplified version
print("Dashboard 3: Top Tracks (like Wrapped) \n")
top_tracks_query = events_df \
    .filter(col("action_type") == "play") \
    .groupBy("track_title", "artist_name") \
    .agg(
        count("*").alias("play_count"),
        avg("play_duration").alias("avg_play_duration_ms"),
    ) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("top_tracks") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation","/tmp/spark_checkpoint/top_tracks") \
    .start()


#Dashboard 4 : User listening stats
print("Dashboard 4: User Listening Stats \n")
user_stats_query = events_df \
    .filter(col("action_type") == "play") \
    .groupBy("user_id") \
    .agg(
        count("*").alias("total_plays"),
        _sum("play_duration").alias("total_play_duration_ms"),
        approx_count_distinct("artist_name", 0.05).alias("unique_artists_listened"),
        approx_count_distinct("genre", 0.05).alias("unique_genres_listened")
    ) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("user_listening_stats") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation","/tmp/spark_checkpoint/user_listening_stats") \
    .start()        


#dashboard 5: Genre popularity over time
print("Dashboard 5: Genre Popularity Over Time \n")
genre_query = events_df \
    .filter(col("action_type") == "play") \
    .groupBy("genre") \
    .agg(
        count("*").alias("play_count"),
        _sum("play_duration").alias("total_play_duration_ms"),
        approx_count_distinct("user_id", 0.05).alias("unique_listeners_approx")
    ) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("genre_popularity") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation","/tmp/spark_checkpoint/genre_popularity") \
    .start()

#dashboard 6: Real-time listening window
print("Dashboard 6: Real-time Listening Window \n")
windowed_query = events_df \
    .filter(col("action_type") == "play") \
    .groupBy(
        window(col("event_time"), "5 minutes" ),
        "genre"
    ) \
    .agg(count("*").alias("play_count")) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("realtime_listening_window  ") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation","/tmp/spark_checkpoint/realtime_listening_window") \
    .start()            


print("="*80  )
print("\n All dashboards launched:\n")
print("Available in memory tables for querying:\n")
print("1. Raw events (for debugging) - query name: streaming_activity")
print("2. Top Artists (like Wrapped) - query name: top_artists" )
print("3. Top Tracks (like Wrapped) - query name: top_tracks")
print("4. User Listening Stats - query name: user_listening_stats")
print("5. Genre Popularity Over Time - query name: genre_popularity")
print("6. Real-time Listening Window - query name: realtime_listening_window")

print("="*80  )
print("Note: Using approx_count_distinct for unique counts to optimize performance. Results are approximate.\n")
print("="*80  )


#auto-refreshing the display

print("\n AUTO-REFRESHING DASHBOARD DISPLAY (Press Ctrl+C to stop) \n")
print("Note: The display will refresh every 8 seconds. You can query the in-memory tables using Spark SQL for more detailed analysis.\n")
print("="*80  )


os.system("mkdir -p /tmp/spark_checkpoint")  # Ensure checkpoint directory exists

try:
    while True:
        clear_screen()
        print("="*80  )
        print("Real-time Music Streaming Dashboard - timestamp: {}".format(time.strftime("%H:%M:%S")))
        print("="*80  )

        # 1. Display Raw Events
        print("\nDashboard 1: Raw Events (for debugging) - query name: streaming_activity\n")
        print("="*80)
        raw_events = spark.sql("SELECT * FROM streaming_activity ORDER BY timestamp DESC LIMIT 5")
        raw_events.show(truncate=False)

        # 2. Display Top Artists
        print("\nDashboard 2: Top Artists (like Wrapped) - query name: top_artists\n")
        print("="*80)
        trending = spark.sql("""
                            select
                                artist_name,
                                play_count,
                                round(total_play_duration_ms/6000,2) as total_play_duration_min,
                                unique_listeners_approx as unique_listeners
                            from top_artists
                            order by play_count desc
                            limit 5
                        """)
        if trending.count() >0:
            trending.show(truncate=False)

        else:
            print("No data available yet. Waiting for events to be processed...")


        # 3.Display Top Tracks
        print("\nDashboard 3: Top Tracks (like Wrapped) - query name: top_tracks\n")
        print("="*80)
        top_tracks = spark.sql("""
                            select
                                track_title,
                                artist_name,
                                play_count,
                                round(avg_play_duration_ms/6000,2) as avg_play_duration_min
                            from top_tracks
                            order by play_count desc
                            limit 5
                        """)
        if top_tracks.count() >0:
            top_tracks.show(truncate=False)
        else:
            print("No data available yet. Waiting for events to be processed...")



        #4. Top Users
        print("\nDashboard 4: User Listening Stats - query name: user_listening_stats\n")
        print("="*80)
        top_users = spark.sql("""
                            select
                                user_id,
                                total_plays,
                                round(total_play_duration_ms/6000,2) as total_play_duration_min,
                                unique_artists_listened as unique_artists
                            from user_listening_stats
                            order by total_plays desc
                            limit 5
                        """)
        if top_users.count() >0:
            top_users.show(truncate=False)
        else:
            print("No data available yet. Waiting for events to be processed...")

        #5. Genre Popularity
        print("\nDashboard 5: Genre Popularity Over Time - query name: genre_popularity\n")
        print("="*80)
        genre_popularity = spark.sql("""
                            select
                                genre,
                                play_count,
                                round(100*play_count/sum(play_count) over(),2) as play_percentage,
                                round(total_play_duration_ms/6000,2) as total_play_duration_min
                            from genre_popularity
                            order by play_count desc
                            limit 5
                        """)
        if genre_popularity.count() >0:
            genre_popularity.show(truncate=False)
        else:
            print("No data available yet. Waiting for events to be processed...")


        #6. Recent Listening Window
        print("\nDashboard 6: Real-time Listening Window - query name: realtime_listening_window\n")
        print("="*80)
        recent_window = spark.sql("""
                            select
                                user_id,
                                artist_name,
                                track_title,
                                round(play_duration/6000,2) as play_duration_min,
                                device,
                                DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm:ss') as event_time
                            from streaming_activity
                            where action_type = 'play'
                            order by event_time desc
                            limit 5
                        """)
        if recent_window.count() >0:
            recent_window.show(truncate=False)
        else:
            print("No data available yet. Waiting for events to be processed...")

        #7. Real-time Listening Stats
        print("Session  Statistics")
        print("=" * 80)
        stats =  spark.sql("""
                        select count(*) as total_plays,
                        round(avg(play_duration)/1000,1) as avg_track_seconds
                        from  streaming_activity
                        where action_type = "play"
                        """)
        stats.show(truncate  = False)

        #8. Current Threads
        print("\n Current trending")
        print("="*80)
        trending_now =  spark.sql("""
                                SELECT  genre, sum(play_count) as current_plays
                                from realtime_listening_window
                                group by genre
                                order by  current_plays desc
                                limit 3
                            """)
        if trending_now.count() >0:
            trending_now.show(truncate= False)

        print("\n" +"="*80)
        print("next update in 8 seconds..... (ctrl+c) for interactive mode")
        print("="*80)

        time.sleep(8)


except KeyboardInterrupt:
    print("\n Entering interactive query name")
    print("="*80)
    print("\n Dashboard are still running! You can query the with SQL.")
    print("\n Try these  examples ")

    while True:
        try:
            query = input("\n SQL query (or 'exit' ):").strip()
            if query.lower() == 'exit':
                print("\n Stopping All Dashboards...")
                activity_query.stop()
                top_artists_query.stop()
                top_tracks_query.stop()
                user_stats_query.stop()
                genre_query.stop()
                windowed_query.stop()
                spark.stop()
                print("ALL st0pped")
                break
            if query:
                print(f"\n Executing: {query}")
                try:
                    result = spark.sql(query)
                    result.show(truncate = False, n= 20)
                except Exception as e:
                    print(f" Error:  {e}")
        except KeyboardInterrupt:
            print("\n forced exit:  ")
            activity_query.stop()
            top_artists_query.stop()
            top_tracks_query.stop()
            user_stats_query.stop()
            genre_query.stop()
            windowed_query.stop()
            spark.stop()
            print("ALL stopped")
            break