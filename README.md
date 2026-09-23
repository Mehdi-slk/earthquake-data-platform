# USGS Earthquake Batch Data Platform

A batch data engineering project that collects earthquake data from the USGS, serializes the data with Avro, publishes it to Kafka, processes it with Apache Spark, loads it into PostgreSQL, and transforms it into a dimensional model with dbt. Apache Airflow orchestrates the pipeline.

This project is intentionally designed as a **batch pipeline**. Spark consumes data from Kafka using the batch DataFrame API rather than Structured Streaming.

## Pipeline

**USGS API → Python → Avro + Schema Registry → Kafka → PySpark batch → PostgreSQL → dbt → Star Schema**

## Technologies

* **Python** — data extraction and Kafka production
* **Apache Kafka** — message transport between ingestion and Spark
* **Apache Avro** — event serialization
* **Confluent Schema Registry** — schema management
* **Apache Spark / PySpark** — batch processing and transformation
* **PostgreSQL** — storage of processed earthquake data
* **dbt** — SQL transformations, dimensional modeling, data quality tests, and model contracts
* **Apache Airflow** — workflow orchestration
* **Docker / Docker Compose** — containerized local environment
* **Git / GitHub** — version control

## Pipeline stages

### 1. Extract earthquake data

Python code in `extract_data/` retrieves earthquake data from the USGS API.

### 2. Serialize and produce data

The producer code in `producers/` handles the Kafka production process.

Earthquake records are serialized using the Avro schema:

```text
schemas/earthquake.avsc
```

Schema Registry is used for schema management.

The serialized records are published to Kafka.

### 3. Batch processing with PySpark

PySpark consumes the Kafka data using the Kafka **batch** source:

```python
spark.read.format("kafka")
```

The Spark processing code transforms the earthquake records before loading them into PostgreSQL.

Geographic data used by the processing layer is located under:

```text
data/geography/
```

### 4. Load into PostgreSQL

The processed earthquake data is loaded into PostgreSQL.

The database table definition is located at:

```text
sql/create_earthquakes_table.sql
```

The earthquake `event_id` is used to prevent duplicate records from being inserted.

When an incoming event already exists in PostgreSQL, the existing record is preserved and the duplicate incoming record is skipped.

### 5. Transform with dbt

dbt transforms the PostgreSQL data into a dimensional model.

The project contains a staging model and the following mart models:

* `date_dim`
* `network_dim`
* `location_dim`
* `event_info_dim`
* `fact_table`

The fact table has the grain:

**one row per earthquake event**

## dbt Star Schema

### `fact_table`

The fact table contains one row per earthquake event.

It contains:

* `event_id`
* `date_key`
* `network_event_code`
* `location_key`
* `event_time`
* `updated_time`
* `magnitude`
* `felt_reports`
* `community_intensity`
* `mercalli_intensity`
* `tsunami_flag`
* `significance_score`
* `station_count`
* `nearest_station_distance`
* `rms_residual`
* `azimuthal_gap`
* `depth`
* `longitude`
* `latitude`

### `date_dim`

Contains date information associated with the earthquake event:

* `date_key`
* `full_date`
* `the_year`
* `the_month`
* `the_day`

### `network_dim`

Contains information about the network that reported the earthquake:

* `network_event_code`
* `source_network`
* `id_sources`

### `location_dim`

Contains geographic information associated with the earthquake:

* `location_key`
* `country`
* `region`

### `event_info_dim`

Contains descriptive information about the earthquake event:

* `event_id`
* `event_status`
* `event_type`
* `event_title`
* `magnitude_type`
* `available_data_types`
* `associated_ids`

## Data quality

The dbt models use several types of data quality tests:

* `not_null`
* `unique`
* `accepted_values`
* `relationships`

Examples include:

* Unique earthquake `event_id` values
* Valid `event_status` values
* Valid `tsunami_flag` values
* Foreign-key relationships between the fact table and dimensions
* Required fields such as event time, updated time, depth, latitude, and longitude

The mart models also use dbt **model contracts** with:

```yaml
contract:
  enforced: true
```

The project uses the `dbt_utils` package.

## Airflow orchestration

The Airflow DAG is located at:

```text
airflow/dags/earthquake_dag.py
```

The DAG is named:

```text
earthquake_data_etl
```

The pipeline contains the following tasks:

```text
extract_produce_data
transform_load_data
build_and_fill_star_schema
```

These tasks cover:

1. Extracting and producing earthquake data
2. Transforming and loading the data
3. Building and filling the dbt star schema

## Project structure

```text
earthquake-data-platform/
│
├── airflow/
│   ├── dags/
│   │   └── earthquake_dag.py
│   └── Dockerfile
│
├── data/
│   ├── batch_offsets.json
│   └── geography/
│
├── dbt/
│   ├── earthquake_dbt/
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   │   ├── staging_table.sql
│   │   │   │   └── schema.yml
│   │   │   ├── marts/
│   │   │   │   ├── date_dim.sql
│   │   │   │   ├── event_info_dim.sql
│   │   │   │   ├── location_dim.sql
│   │   │   │   ├── network_dim.sql
│   │   │   │   ├── fact_table.sql
│   │   │   │   └── schema.yml
│   │   │   └── sources.yml
│   │   ├── macros/
│   │   ├── seeds/
│   │   ├── snapshots/
│   │   ├── tests/
│   │   ├── packages.yml
│   │   └── dbt_project.yml
│   └── profiles/
│       └── profiles.yml
│
├── extract_data/
│   ├── earthquake_data_extract.py
│   └── test.py
│
├── producers/
│   ├── earthquake_producer.py
│   └── producing_data.py
│
├── producerss/
│   └── earthquake_producer.py
│
├── schemas/
│   └── earthquake.avsc
│
├── scripts/
│   ├── earthquake_data.py
│   └── earthquake_data_1.py
│
├── spark/
│   ├── Dockerfile
│   └── earthquake_data.py
│
├── sql/
│   └── create_earthquakes_table.sql
│
├── Dockerfile
├── docker-compose.yml
└── .gitignore
```

## Running the project

The project uses Docker Compose for the local environment.

### Run the producer

```bash
python3 -m producers.producing_data
```

### Run the Spark batch job

```bash
docker compose exec spark-driver /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/earthquake_data_1.py
```

### Run dbt

```bash
docker exec dbt dbt build --project-dir /usr/app/earthquake_dbt
```

To perform a full refresh:

```bash
docker exec dbt dbt build --project-dir /usr/app/earthquake_dbt --full-refresh
```

## Design considerations

### Batch rather than streaming

Kafka is used as part of the ingestion pipeline, but this project is not a streaming pipeline.

Spark reads Kafka data with:

```python
spark.read.format("kafka")
```

rather than:

```python
spark.readStream
```

This allows Kafka to be used as the handoff point between ingestion and the Spark batch processing stage.

### Duplicate event handling

The PostgreSQL earthquake table uses `event_id` as the unique identifier for an earthquake event.

Before inserting new records, existing event IDs are read from PostgreSQL and removed from the incoming Spark DataFrame using an anti-join.

This means that when the same earthquake appears again:

**the existing PostgreSQL record is kept and the incoming duplicate is ignored.**

### Dimensional modeling

The final analytical layer separates earthquake information into dimensions and a fact table.

The fact table represents the earthquake event grain, while descriptive information is organized into date, network, location, and event-information dimensions.

## Project objective

The objective of this project is to implement an end-to-end **batch data engineering pipeline** using multiple components commonly found in modern data platforms:

* API ingestion
* Data serialization
* Schema management
* Kafka
* Distributed batch processing with Spark
* PostgreSQL
* Dimensional modeling
* dbt data quality
* Airflow orchestration
* Docker
* Git
