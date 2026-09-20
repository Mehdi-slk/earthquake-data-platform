from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,coalesce, regexp_replace, col, from_unixtime, date_format, when, to_timestamp,year,month, avg, min, max, sum, row_number, lag, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,ArrayType,LongType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName(
    "Read_earthquake_data_from_kafka"
).getOrCreate()

earthquake_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("event_time", LongType(), False),
    StructField("updated_time", LongType(), False),
    StructField("tz", IntegerType(), True),
    StructField("felt", IntegerType(), True),
    StructField("cdi", DoubleType(), True),
    StructField("mmi", DoubleType(), True),
    StructField("alert", StringType(), True),
    StructField("status", StringType(), True),
    StructField("tsunami", IntegerType(), False),
    StructField("significance", IntegerType(), False),
    StructField("network", StringType(), True),
    StructField("code", StringType(), True),
    StructField("ids", StringType(), True),
    StructField("sources", StringType(), True),
    StructField("types", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("magnitude_type", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("longitude", DoubleType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("depth", DoubleType(), False)
])

read_df = spark.read.format('kafka') \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "earthquakes") \
    .option("startingOffsets", "earliest") \
    .load()
read_df.show(5)