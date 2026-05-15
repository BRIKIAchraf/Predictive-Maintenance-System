import time
import json
import random
import numpy as np
from paho.mqtt import client as mqtt_client

# Configuration
BROKER = 'localhost'
PORT = 1883
TOPIC_PREFIX = "factory/sensor"
FACTORY_ID = "factory_01"
MACHINES = ["m_01", "m_02", "m_03"]

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(f'simulator_{random.randint(0, 1000)}')
    client.on_connect = on_connect
    client.connect(BROKER, PORT)
    return client

class MachineSimulator:
    def __init__(self, machine_id):
        self.machine_id = machine_id
        self.base_temp = 65.0
        self.base_vibration = 0.5
        self.base_power = 1200.0
        self.base_rpm = 1750.0
        self.base_current = 4.2
        self.state = "NORMAL"

    def generate_data(self):
        # Add random noise
        vibration = self.base_vibration + np.random.normal(0, 0.05)
        temperature = self.base_temp + np.random.normal(0, 0.5)
        power = self.base_power + np.random.normal(0, 20)
        rpm = self.base_rpm + np.random.normal(0, 5)
        current = self.base_current + np.random.normal(0, 0.1)

        # Occasional anomaly simulation
        if random.random() < 0.02:  # 2% chance of spike
            vibration *= 3.0
            temperature += 15.0
            current *= 1.5
            self.state = "WARNING"
        else:
            self.state = "NORMAL"

        return {
            "timestamp": int(time.time() * 1000),
            "machine_id": self.machine_id,
            "factory_id": FACTORY_ID,
            "vibration": round(float(vibration), 4),
            "temperature": round(float(temperature), 2),
            "power_consumption": round(float(power), 2),
            "rpm": round(float(rpm), 1),
            "current_amps": round(float(current), 2),
            "status": self.state
        }

def run():
    client = connect_mqtt()
    client.loop_start()
    
    simulators = [MachineSimulator(m) for m in MACHINES]
    
    print(f"Starting simulation for {len(MACHINES)} machines...")
    
    try:
        while True:
            for sim in simulators:
                data = sim.generate_data()
                topic = f"{TOPIC_PREFIX}/{sim.machine_id}"
                result = client.publish(topic, json.dumps(data))
                status = result[0]
                if status == 0:
                    # print(f"Sent {topic}: {data['status']}")
                    pass
                else:
                    print(f"Failed to send message to topic {topic}")
            
            time.sleep(1)  # 1 Hz sampling rate
    except KeyboardInterrupt:
        print("Simulation stopped.")
        client.loop_stop()

if __name__ == '__main__':
    run()
