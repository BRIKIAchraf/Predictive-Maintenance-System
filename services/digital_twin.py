import json
import time
from fastapi import FastAPI
from confluent_kafka import Consumer
import threading

app = FastAPI(title="Industrial Digital Twin Engine")

# Virtual State Store (In-memory for demo, should use Redis/Postgres)
TWIN_STORE = {}

# Configuration
KAFKA_BROKER = "localhost:29092"
TELEMETRY_TOPIC = "factory.sensor.telemetry"

class MachineTwin:
    def __init__(self, machine_id):
        self.machine_id = machine_id
        self.total_runtime_hours = 0.0
        self.current_wear_level = 0.0  # 0 to 100
        self.last_update = time.time()
        self.health_score = 100.0
        self.predicted_failure_hours = 5000.0

    def update(self, telemetry):
        # Calculate time delta
        now = time.time()
        dt = (now - self.last_update) / 3600.0  # hours
        self.last_update = now
        
        # Physics-based wear simulation (simplified)
        # Wear increases faster with higher vibration and temperature
        vibration = telemetry.get('vibration', 0.5)
        temp = telemetry.get('temperature', 65.0)
        
        wear_rate = 0.01  # base wear per hour
        if vibration > 1.0: wear_rate *= 5.0
        if temp > 80.0: wear_rate *= 3.0
        
        self.total_runtime_hours += dt
        self.current_wear_level += wear_rate * dt
        
        # Calculate Health Score
        self.health_score = max(0, 100.0 - self.current_wear_level)
        
        # Remaining Useful Life (RUL) estimation
        self.predicted_failure_hours = max(0, (100.0 - self.current_wear_level) / (wear_rate + 0.0001))

    def to_dict(self):
        return self.__dict__

def twin_consumer_loop():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'digital-twin-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([TELEMETRY_TOPIC])
    
    print(f"Digital Twin Engine started. Monitoring {TELEMETRY_TOPIC}")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        try:
            # Note: In real setup, would decode Avro
            telemetry = json.loads(msg.value().decode())
            m_id = telemetry['machine_id']
            
            if m_id not in TWIN_STORE:
                TWIN_STORE[m_id] = MachineTwin(m_id)
            
            TWIN_STORE[m_id].update(telemetry)
            
        except Exception as e:
            print(f"Twin update error: {e}")

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=twin_consumer_loop, daemon=True)
    thread.start()

@app.get("/twins/{machine_id}")
def get_twin(machine_id: str):
    twin = TWIN_STORE.get(machine_id)
    if not twin:
        return {"error": "Machine Twin not found"}
    return twin.to_dict()

@app.get("/dashboard")
def get_dashboard():
    return {m_id: twin.to_dict() for m_id, twin in TWIN_STORE.items()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
