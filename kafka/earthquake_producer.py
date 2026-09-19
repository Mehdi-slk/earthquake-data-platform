from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from extract_data.earthquake_data_extract import extract_earthquakes
import logging
import  os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = {
    'bootstrap.servers' : os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'localhost:9092'
    ),
    'enable.idempotence' : True,
    'linger.ms' : 10,
    'batch.size' : 16384,
    'retries' : 2147483647,
    'max.in.flight.requests.per.connection': 5,
    'delivery.timeout.ms': 30000,
    'retry.backoff.ms' : 1000
}

schema_registry_conf = {
    "url": os.getenv(
        "SCHEMA_REGISTRY_URL",
        "http://localhost:8083"
    )
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("schemas/earthquake.avsc", "r") as schema_file:
    earthquake_schema = schema_file.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    earthquake_schema
)


def delivery_report(err,msg):
    if err is not None:
        logger.error(
            f"Delivery failed for record {msg.key()}: {err}"
        )
    else:
        logger.info(
            f"record {msg.key()} successfully produced"
            f"to {msg.topic()} [{msg.partition()}]"
            f"at offset {msg.offset()}"
        )

producer = Producer(config)

data = extract_earthquakes('2026-08-01', '2026-09-19')

try:
    for earthquake in data:
        

        while True : 
            try:
                producer.produce(
                    topic= 'earthquakes',
                    key=earthquake["event_id"],
                    value=avro_serializer(earthquake, SerializationContext('earthquakes', MessageField.VALUE)),
                    on_delivery=delivery_report
                )
                producer.poll(0)
                break
            except BufferError:
                producer.poll(1.0)
                continue
    producer.flush()

    logger.info("all earyhquake data have been flushed")
finally:
    producer.flush()