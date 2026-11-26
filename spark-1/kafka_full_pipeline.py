import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,expr, lit,current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, LongType

def main():
    spark = SparkSession.builder \
        .appName("KafkaSparkStreamingProductPipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting Kafka-Spark Streaming Product Pipeline...")
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    # Define the schema for the incoming JSON data
    event_schema = StructType([
        StructField("user", StringType(), True),
        StructField("page", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])      
    
      # Parse the JSON data and select relevant fields
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_time", current_timestamp())

    #Transform - Add Business logic
    #Example: Categorize pages , add flags, calculate session metrics

    enriched_df = parsed_df.withColumn("page_category", expr("""
        CASE
            WHEN page LIKE '/home%' THEN 'product_view'
            WHEN page LIKE '/about%' THEN 'cart_interaction'
            WHEN page LIKE '/checkout%' THEN 'checkout_process'
            ELSE 'browsing'
        END
    """)) \
    .withColumn('is_conversion', expr("CASE WHEN page = '/about' then 1 ELSE 0 END")) \
    .withColumn("data_source", lit("web_stream")) \
    .withColumn("event_id", expr("uuid()"))

    #aggregate  - simple widowed aggregation example
    #this shows the power of spark structured streaming to do aggregations on streaming data

    windowed_counts = enriched_df \
        .withWatermark("processed_time", "1 minute") \
        .groupBy(
            window(col("processed_time"), "30 seconds"),
            'page_category'
        ) \
        .count() \
        .withColumnRenamed("count", "event_count")

    #write to multiple sinks (simultaneously) - console, kafka, parquet files
    console_query = enriched_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    #write back to kafka topic for downstream processing
    kafka_query = enriched_df \
    .selectExpr('to_json(struct(*)) AS value') \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed-events") \
    .option("checkpointLocation", "/tmp/kafka_spark_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()


    #write to windowed aggregations console
    aggregate_query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .start()

    print('\n' + '="'*50 + '=\n')
    print("1.raw events from kafka topic 'user-events' are being processed and enriched.")
    print("2.enriched events are being written to kafka topic 'processed-events'." )
    print("3.windowed aggregations are being printed to console every 30 seconds." )
    print('\n' + '="'*50 + '=\n'   )
    print(' docker exec -it kafka kafka-console-consumer \\')
    print(' --bootstrap-server localhost:9092 \\'  )
    print(' --topic processed-events \\'  )
    print(' --from-beginning \\'  )


    #Monitor the streaming queries and wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
        for query in spark.streams.active:
            query.stop()    
        print("All streaming queries stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()