from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime,
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType
)
from pyspark.sql.avro.functions import from_avro

import geopandas as gpd
from shapely.geometry import Point


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


# ---------------------------------------------------------
# Read earthquakes from Kafka
# ---------------------------------------------------------
import json

with open("/opt/airflow/data/batch_offsets.json", "r") as f:
    batch_offsets = json.load(f)

starting_offsets = {
    "earthquakes": {
        str(partition): offsets["start"]
        for partition, offsets in batch_offsets.items()
    }
}

ending_offsets = {
    "earthquakes": {
        str(partition): offsets["end"] + 1
        for partition, offsets in batch_offsets.items()
    }
}

read_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9093")
    .option("subscribe", "earthquakes")
    .option("startingOffsets", json.dumps(starting_offsets))
    .option("endingOffsets", json.dumps(ending_offsets))
    .load()
)

with open("/opt/airflow/schemas/earthquake.avsc", "r") as f:
    avro_schema = f.read()


decoded_df = read_df.select(
    from_avro(
        col("value").substr(6, 1000000),
        avro_schema
    ).alias("earthquake")
)


# ---------------------------------------------------------
# Transform earthquake data
# ---------------------------------------------------------

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
        to_timestamp(
            from_unixtime(col("event_time") / 1000)
        )
    )
    .withColumn(
        "updated_time",
        to_timestamp(
            from_unixtime(col("updated_time") / 1000)
        )
    )
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
)


# ---------------------------------------------------------
# Load geographic data
# ---------------------------------------------------------

admin1_df = gpd.read_file(
    "/opt/airflow/data/geography/admin1.geojson"
)

admin1_df = admin1_df[
    ["admin", "name", "geometry"]
].rename(
    columns={
        "admin": "country",
        "name": "region"
    }
)


country_df = gpd.read_file(
    "/opt/airflow/data/geography/ne_10m_admin_0_countries.shp"
)

country_df = country_df[
    ["ADMIN", "geometry"]
].rename(
    columns={
        "ADMIN": "country"
    }
)


# ---------------------------------------------------------
# Create geographic points
# ---------------------------------------------------------

points = transformed_df.select(
    "event_id",
    "latitude",
    "longitude"
).toPandas()


points_gdf = gpd.GeoDataFrame(
    points,
    geometry=[
        Point(lon, lat)
        for lat, lon in zip(
            points["latitude"],
            points["longitude"]
        )
    ],
    crs="EPSG:4326"
)


# ---------------------------------------------------------
# 1. Exact administrative region
# ---------------------------------------------------------

admin1_matches = gpd.sjoin(
    points_gdf,
    admin1_df,
    how="left",
    predicate="intersects"
)

admin1_matches = admin1_matches[
    ["event_id", "country", "region"]
].drop_duplicates("event_id")


# ---------------------------------------------------------
# 2. Exact country
# ---------------------------------------------------------

country_matches = gpd.sjoin(
    points_gdf,
    country_df,
    how="left",
    predicate="intersects"
)

country_matches = country_matches[
    ["event_id", "country"]
].drop_duplicates("event_id")


# ---------------------------------------------------------
# 3. Offshore country fallback
# ---------------------------------------------------------

unmatched_country_points = points_gdf[
    ~points_gdf["event_id"].isin(
        country_matches[
            country_matches["country"].notna()
        ]["event_id"]
    )
].copy()


if not unmatched_country_points.empty:

    points_projected = unmatched_country_points.to_crs(
        "ESRI:54009"
    )

    countries_projected = country_df.to_crs(
        "ESRI:54009"
    )

    nearest_country_matches = gpd.sjoin_nearest(
        points_projected,
        countries_projected[
            ["country", "geometry"]
        ],
        how="left"
    )

    nearest_country_matches = nearest_country_matches[
        ["event_id", "country"]
    ].drop_duplicates("event_id")

    country_matches = country_matches.merge(
        nearest_country_matches,
        on="event_id",
        how="left",
        suffixes=("_exact", "_nearest")
    )

    country_matches["country"] = (
        country_matches["country_exact"]
        .fillna(country_matches["country_nearest"])
    )

    country_matches = country_matches[
        ["event_id", "country"]
    ]


# ---------------------------------------------------------
# 4. Combine country with exact region
# ---------------------------------------------------------

geographic_data = country_matches.merge(
    admin1_matches,
    on="event_id",
    how="left",
    suffixes=("_country", "_region")
)

geographic_data["country"] = (
    geographic_data["country_country"]
)

geographic_data["region"] = (
    geographic_data["region"]
)


# ---------------------------------------------------------
# 5. Offshore region fallback
# ---------------------------------------------------------

unmatched_region = geographic_data[
    geographic_data["region"].isna()
    | (geographic_data["region"] == "nan")
].copy()


if not unmatched_region.empty:

    unmatched_region_points = points_gdf[
        points_gdf["event_id"].isin(
            unmatched_region["event_id"]
        )
    ].copy()

    unmatched_region_points = unmatched_region_points.merge(
        geographic_data[
            ["event_id", "country"]
        ],
        on="event_id",
        how="left"
    )

    nearest_region_results = []

    # Find the nearest region only inside
    # the earthquake's already assigned country.
    for country_name in (
        unmatched_region_points["country"]
        .dropna()
        .unique()
    ):

        country_points = unmatched_region_points[
            unmatched_region_points["country"] == country_name
        ].copy()

        country_regions = admin1_df[
            admin1_df["country"] == country_name
        ].copy()

        if country_regions.empty:
            continue

        points_projected = country_points.to_crs(
            "ESRI:54009"
        )

        regions_projected = country_regions.to_crs(
            "ESRI:54009"
        )

        matches = gpd.sjoin_nearest(
            points_projected,
            regions_projected[
                ["region", "geometry"]
            ],
            how="left"
        )

        nearest_region_results.append(
            matches[
                ["event_id", "region"]
            ]
        )

    if nearest_region_results:

        import pandas as pd

        nearest_region_matches = pd.concat(
            nearest_region_results,
            ignore_index=True
        )

        nearest_region_matches = (
            nearest_region_matches
            .rename(
                columns={
                    "region": "nearest_region"
                }
            )
        )

        geographic_data = geographic_data.merge(
            nearest_region_matches[
                ["event_id", "nearest_region"]
            ],
            on="event_id",
            how="left"
        )

        geographic_data["region"] = (
            geographic_data["region"]
            .replace("nan", None)
            .fillna(
                geographic_data["nearest_region"]
            )
        )

        geographic_data = geographic_data.drop(
            columns=["nearest_region"]
        )


# ---------------------------------------------------------
# Final geographic data
# ---------------------------------------------------------

geographic_data["country"] = (
    geographic_data["country"]
    .replace("nan", None)
)

geographic_data["region"] = (
    geographic_data["region"]
    .replace("nan", None)
)

geographic_data = geographic_data[
    ["event_id", "country", "region"]
]


geographic_spark_df = spark.createDataFrame(
    geographic_data
)


# ---------------------------------------------------------
# Join geographic data back to Spark DataFrame
# ---------------------------------------------------------

transformed_df = transformed_df.join(
    geographic_spark_df,
    on="event_id",
    how="left"
)


# ---------------------------------------------------------
# Write to PostgreSQL
# ---------------------------------------------------------
existing_ids = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/earthquake") \
    .option(
        "dbtable",
        "(SELECT event_id FROM earthquake.earthquakes) AS existing"
    ) \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

transformed_df = transformed_df.join(
    existing_ids,
    on="event_id",
    how="left_anti"
)
transformed_df.write \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://postgres:5432/earthquake"
    ) \
    .option(
        "dbtable",
        "earthquake.earthquakes"
    ) \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()