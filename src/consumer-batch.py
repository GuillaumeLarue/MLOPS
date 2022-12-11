from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

sc = SparkSession.builder \
    .appName("ConsumerBatch") \
    .getOrCreate()

sc.sparkContext.setLogLevel("ERROR")

conf = SparkConf() \
    .set("spark.cassandra.connection.host", "localhost") \
    .set("spark.cassandra.connection.port", "9042") \
    .set("spark.cassandra.auth.username", "cassandra") \
    .set("spark.cassandra.auth.password", "cassandra")

spark = SparkSession.builder \
    .appName("ConsumerBatch") \
    .config(conf=conf) \
    .getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("time", TimestampType()),
    StructField("temp", FloatType())
])

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "temperature") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
# .option("startingOffsets", "latest") \
# .option("endingOffsets", "latest") \

df = df.select(from_json(col("value").cast("string"), schema).alias("json_data"))
# extract the columns from the JSON data
import uuid
from pyspark.sql.functions import udf

df = df.select("json_data.*")

uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
df = df.withColumn("uuid", uuidUdf())
df = df.withColumnRenamed("id", "sensor_id")
df = df.withColumnRenamed("temp", "value")
df = df.withColumn("type", lit(str("temperature")[:10]))
# df.printSchema()
# uuid, sensor_id, time, type, value

query = df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("keyspace", "mlops") \
    .option("table", "logs") \
    .option("interval", "30 seconds") \
    .save()

"""
def writeToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="randintstream", keyspace="kafkaspark") \
        .mode("append") \
        .save()


query = df.writeStream \
    .trigger(processingTime="5 seconds") \
    .outputMode("update") \
    .foreachBatch(writeToCassandra) \
    .start()
"""
# To run the script:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/consumer-batch.py

# Scala version : 2.12.17
# Spark version : 3.3.1
# Kafka version : 3.3.1
# spark.sparkContext.addPyFile("/path/to/spark-cassandra-connector-assembly.jar")
