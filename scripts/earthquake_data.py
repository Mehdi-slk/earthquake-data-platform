from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,coalesce, regexp_replace, col, from_unixtime, date_format, when, to_timestamp,year,month, avg, min, max, sum, row_number, lag, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,ArrayType,LongType
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
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


with open("/opt/spark/schemas/earthquake.avsc", "r") as f:
    avro_schema = f.read()

decoded_df = read_df.select(
    from_avro(
        col("value").substr(6,1000000),
        avro_schema
    ).alias("earthquake")
)

from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour
)

transformed_df = (
    decoded_df
    .select(
        col("earthquake.event_id").alias("event_id"),
        col("earthquake.magnitude").alias("magnitude"),
        col("earthquake.place").alias("place"),
        col("earthquake.event_time").alias("event_time"),
        col("earthquake.updated_time").alias("updated_time"),
        col("earthquake.tz").alias("tz"),
        col("earthquake.felt").alias("felt"),
        col("earthquake.cdi").alias("cdi"),
        col("earthquake.mmi").alias("mmi"),
        col("earthquake.alert").alias("alert"),
        col("earthquake.status").alias("status"),
        col("earthquake.tsunami").alias("tsunami"),
        col("earthquake.significance").alias("significance"),
        col("earthquake.network").alias("network"),
        col("earthquake.code").alias("code"),
        col("earthquake.ids").alias("ids"),
        col("earthquake.sources").alias("sources"),
        col("earthquake.types").alias("types"),
        col("earthquake.nst").alias("nst"),
        col("earthquake.dmin").alias("dmin"),
        col("earthquake.rms").alias("rms"),
        col("earthquake.gap").alias("gap"),
        col("earthquake.magnitude_type").alias("magnitude_type"),
        col("earthquake.event_type").alias("event_type"),
        col("earthquake.title").alias("title"),
        col("earthquake.longitude").alias("longitude"),
        col("earthquake.latitude").alias("latitude"),
        col("earthquake.depth").alias("depth")
    )
    .withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_time") / 1000))
    )
    .withColumn(
        "updated_time",
        to_timestamp(from_unixtime(col("updated_time") / 1000))
    )
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
)

transformed_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/earthquake") \
    .option("dbtable", "earthquake.earthquakes") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()