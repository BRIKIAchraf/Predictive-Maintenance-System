import json
import sqlite3
import time
import os
from paho.mqtt import client as mqtt_client
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# --- CONFIGURATION ---
MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
OTLP_ENDPOINT = os.getenv('OTLP_ENDPOINT', 'http://localhost:4317')

DB_PATH = "../data/edge_buffer.db"

# --- OPENTELEMETRY SETUP ---
resource = Resource(attributes={"service.name": "edge-gateway"})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# --- OFFLINE BUFFER SETUP (SQLite) ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS buffer 
                    (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, topic TEXT)''')
    conn.commit()
    return conn

db_conn = init_db()

# --- KAFKA & AVRO SETUP ---
try:
    with open('../schemas/sensor_telemetry.avsc', 'r') as f:
        schema_str = f.read()
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
except Exception as e:
    print(f"Warning: Kafka/Schema Registry not available. Offline mode active. {e}")
    producer = None

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        pass

def process_buffer():
    """Tries to send buffered messages to Kafka when connection is back."""
    if producer is None: return
    
    cursor = db_conn.cursor()
    cursor.execute("SELECT id, payload, topic FROM buffer LIMIT 100")
    rows = cursor.fetchall()
    
    for row in rows:
        msg_id, payload_str, topic = row
        try:
            payload = json.loads(payload_str)
            producer.produce(
                topic="factory.sensor.telemetry",
                key=payload['machine_id'],
                value=avro_serializer(payload, SerializationContext("factory.sensor.telemetry", MessageField.VALUE)),
                on_delivery=delivery_report
            )
            producer.poll(0)
            cursor.execute("DELETE FROM buffer WHERE id = ?", (msg_id,))
            db_conn.commit()
        except Exception as e:
            print(f"Failed to clear buffer: {e}")
            break

def on_message(client, userdata, msg):
    with tracer.start_as_current_span("ingest_mqtt_message") as span:
        try:
            payload = json.loads(msg.payload.decode())
            span.set_attribute("machine_id", payload.get('machine_id', 'unknown'))
            
            if producer:
                try:
                    producer.produce(
                        topic="factory.sensor.telemetry",
                        key=payload['machine_id'],
                        value=avro_serializer(payload, SerializationContext("factory.sensor.telemetry", MessageField.VALUE)),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    # Also check if we have old data in buffer to sync
                    process_buffer()
                except Exception as e:
                    # Connection failed? Save to buffer
                    db_conn.execute("INSERT INTO buffer (payload, topic) VALUES (?, ?)", 
                                  (msg.payload.decode(), msg.topic))
                    db_conn.commit()
            else:
                # No producer? Save to buffer
                db_conn.execute("INSERT INTO buffer (payload, topic) VALUES (?, ?)", 
                              (msg.payload.decode(), msg.topic))
                db_conn.commit()
                
        except Exception as e:
            print(f"Error: {e}")

def run():
    client = mqtt_client.Client("edge_gateway_staff")
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883)
    client.subscribe("factory/sensor/#")
    print("Resilient Edge Gateway with Tracing and Offline Buffering started...")
    client.loop_forever()

if __name__ == '__main__':
    run()
