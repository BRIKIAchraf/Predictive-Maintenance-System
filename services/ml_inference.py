import json
import time
from fastapi import FastAPI
from confluent_kafka import Consumer, Producer
import threading

app = FastAPI(title="Industrial ML Inference Service")

# Configuration
KAFKA_BROKER = "localhost:29092"
FEATURE_TOPIC = "factory.features.realtime"
ALERT_TOPIC = "factory.alerts"

# Kafka Producer for Alerts
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

import mlflow
from opentelemetry import trace

# Initialize Tracing
tracer = trace.get_tracer(__name__)

def detect_anomaly(feature_data):
    """
    Advanced Industrial Anomaly Detection using Z-Score (z = (x - mu) / sigma).
    In production: models are pulled from MLflow Model Registry.
    """
    with tracer.start_as_current_span("ml_inference") as span:
        vibration_rms = feature_data.get('vibration_rms', 0)
        vibration_mean = feature_data.get('vibration_mean', 0.5)
        vibration_std = feature_data.get('vibration_std', 0.05)
        
        # Z-SCORE CALCULATION: z = (x - mu) / sigma
        # This handles environmental variation by normalizing the signal
        z_score = abs(vibration_rms - vibration_mean) / (vibration_std + 0.0001)
        
        span.set_attribute("z_score", z_score)
        
        is_anomaly = False
        reason = ""
        
        # Z > 3 is the industry standard for 99.7% confidence in anomaly detection
        if z_score > 3.0:
            is_anomaly = True
            reason = f"Z-Score Violation ({z_score:.2f}) - Statistical Anomaly"
        elif vibration_rms > 1.8:
            is_anomaly = True
            reason = "Hard Threshold Violation - Critical Energy"
            
        return is_anomaly, reason

def kafka_consumer_loop():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'ml-inference-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([FEATURE_TOPIC])
    
    print(f"ML Inference Consumer started on {FEATURE_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            feature_data = json.loads(msg.value().decode())
            is_anomaly, reason = detect_anomaly(feature_data)
            
            if is_anomaly:
                alert = {
                    "timestamp": int(time.time() * 1000),
                    "machine_id": feature_data['machine_id'],
                    "type": "ANOMALY",
                    "severity": "CRITICAL",
                    "details": reason,
                    "metrics": feature_data
                }
                producer.produce(ALERT_TOPIC, value=json.dumps(alert))
                producer.flush()
                print(f"!!! ALERT !!! {feature_data['machine_id']}: {reason}")
                
    except Exception as e:
        print(f"Consumer loop error: {e}")
    finally:
        consumer.close()

@app.on_event("startup")
def startup_event():
    # Run Kafka consumer in a background thread
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "running", "model_version": "1.0.0-baseline"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
