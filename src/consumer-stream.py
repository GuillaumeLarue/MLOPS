from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

sc = SparkSession.builder \
    .appName("ConsumerBatch") \
    .getOrCreate()

sc.sparkContext.setLogLevel("ERROR")

spark = SparkSession.builder \
    .appName("ConsumerStream") \
    .getOrCreate()

schema = StructType([
    # "id": rnd_id, "time": current_date, "temp"
    StructField("id", StringType()),
    StructField("time", TimestampType()),
    StructField("temp", StringType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "temperature") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \

df = df.select(from_json(col("value").cast("string"), schema).alias("json_data"))
# extract the columns from the JSON data
df = df.select("json_data.*")

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "./tmp/checkpoint") \
    .start()

query.awaitTermination()

# To run the script:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 src/test.py

# Scala version : 2.12.17
# Spark version : 3.3.1
# Kafka version : 3.3.1
