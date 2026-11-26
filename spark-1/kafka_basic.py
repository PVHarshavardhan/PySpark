from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .getOrCreate()
    

raw_df =  spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "earliest") \
    .load()

print("Raw DataFrame Schema:")

raw_df.printSchema()

json_string_df = raw_df.selectExpr("CAST(value AS STRING) as json_string")

event_schema = StructType([
    StructField("user", StringType(), True),
    StructField("page", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", LongType(), True)
])

parsed_df = json_string_df.select(from_json(col("json_string"), event_schema).alias("data")).select("data.*")

print("Parsed DataFrame Schema:")
parsed_df.printSchema()

print("Starting query to write parsed data to console...")
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()


try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping query...")
finally:
    query.stop()
    