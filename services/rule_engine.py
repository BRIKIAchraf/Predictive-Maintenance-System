import json
import time
from confluent_kafka import Consumer, Producer

# Configuration
KAFKA_BROKER = "localhost:29092"
INPUT_TOPIC = "factory.sensor.telemetry"
ALERT_TOPIC = "factory.alerts"

# State Store for CEP
MACHINE_STATES = {}

def process_rules(telemetry):
    m_id = telemetry['machine_id']
    if m_id not in MACHINE_STATES:
        MACHINE_STATES[m_id] = {"vibration_history": [], "temp_history": []}
    
    state = MACHINE_STATES[m_id]
    state["vibration_history"].append(telemetry['vibration'])
    state["temp_history"].append(telemetry['temperature'])
    
    # Keep last 5 samples
    state["vibration_history"] = state["vibration_history"][-5:]
    state["temp_history"] = state["temp_history"][-5:]
    
    # --- RULE 1: Sustained Vibration Spike ---
    if len(state["vibration_history"]) >= 3:
        if all(v > 1.2 for v in state["vibration_history"][-3:]):
            return "CRITICAL", "Sustained high vibration detected over 3 cycles"
            
    # --- RULE 2: Rapid Temperature Rise ---
    if len(state["temp_history"]) >= 3:
        rise = state["temp_history"][-1] - state["temp_history"][-3]
        if rise > 5.0: # 5 degrees rise in 3 cycles
            return "WARNING", f"Rapid temperature rise detected: +{rise:.2f}°C"
            
    return None, None

def run_engine():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'rule-engine-group',
        'auto.offset.reset': 'latest'
    })
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    consumer.subscribe([INPUT_TOPIC])
    
    print("CEP Rule Engine started. Monitoring complex industrial patterns...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            
            telemetry = json.loads(msg.value().decode())
            severity, alert_msg = process_rules(telemetry)
            
            if severity:
                alert = {
                    "timestamp": int(time.time() * 1000),
                    "machine_id": telemetry['machine_id'],
                    "type": "CEP_RULE_MATCH",
                    "severity": severity,
                    "details": alert_msg
                }
                producer.produce(ALERT_TOPIC, value=json.dumps(alert))
                producer.flush()
                print(f"!!! CEP ALERT !!! {telemetry['machine_id']}: {alert_msg}")
                
    except Exception as e:
        print(f"Rule engine error: {e}")

if __name__ == "__main__":
    run_engine()
