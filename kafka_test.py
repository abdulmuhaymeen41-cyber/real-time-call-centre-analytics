
import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer

# Load credentials from our .env file
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("KAFKA_API_KEY")
API_SECRET = os.getenv("KAFKA_API_SECRET")
TOPIC = os.getenv("KAFKA_TOPIC", "call-events")

# Check that all required settings exist
if not all([BOOTSTRAP_SERVERS, API_KEY, API_SECRET, TOPIC]):
    raise ValueError(
        "Missing Kafka configuration. Check your .env file."
    )

# Configure Kafka producer
config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
"sasl.password": API_SECRET,
"message.timeout.ms": 10000,
"debug": "broker,protocol"
}

producer = Producer(config)
producer.list_topics(TOPIC, timeout=10)
print(f"Successfully connected to Kafka topic: {TOPIC}")

# Create a test event
test_event = {
    "event_type": "TEST",
    "message": "Hello from Ubuntu Python!",
    "source": "call-centre-project",
    "timestamp": datetime.now(timezone.utc).isoformat()
}

def delivery_report(err, message):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print("\nMESSAGE SUCCESSFULLY SENT!")
        print(f"Topic: {message.topic()}")
        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")

try:
    producer.produce(
        topic=TOPIC,
        key="connection-test",
        value=json.dumps(test_event),
        callback=delivery_report
    )

    remaining = producer.flush(timeout=15)

    if remaining > 0:
        print(f"{remaining} message(s) still not delivered.")

except Exception as error:
    print(f"Kafka connection error: {error}")

finally:
    print("\nKafka test finished.")

