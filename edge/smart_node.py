import time
import json
import numpy as np
from paho.mqtt import client as mqtt_client

# This simulates a TinyML model (e.g. TensorFlow Lite Micro) running on an ESP32/ARM Cortex
class TinyMLModel:
    def __init__(self):
        self.threshold = 1.2
        print("TinyML Model Initialized (Simulated TFLite Micro)")

    def predict(self, raw_buffer):
        """
        Simulates running inference on a buffer of vibration samples.
        In a real TinyML setup, this would be an FFT + Neural Network.
        """
        # Calculate energy in high frequency (simulated)
        energy = np.mean(np.square(raw_buffer))
        
        # If energy > threshold, local anomaly detected
        if energy > self.threshold:
            return "ANOMALY_DETECTED", float(energy)
        return "NORMAL", float(energy)

def run_smart_node():
    mqtt = mqtt_client.Client("smart_edge_node_01")
    mqtt.connect("localhost", 1883)
    
    model = TinyMLModel()
    
    print("Smart Edge Node started. Performing Local Inference (TinyML)...")
    
    try:
        while True:
            # 1. Collect Raw Samples (e.g. 100 samples of vibration)
            # Simulating high-frequency sampling (1kHz)
            raw_samples = np.random.normal(0.5, 0.2, 100)
            
            # 2. Add a simulated physical anomaly
            if time.time() % 30 < 2:  # Every 30s, create a 2s anomaly
                raw_samples += 1.0
            
            # 3. TinyML Local Inference
            state, score = model.predict(raw_samples)
            
            if state == "ANOMALY_DETECTED":
                # Only publish alerts locally or to the cloud if something is wrong
                # This saves huge bandwidth in real industrial setups
                payload = {
                    "timestamp": int(time.time() * 1000),
                    "node_id": "edge_01",
                    "event": "BEARING_VIBRATION_CRITICAL",
                    "local_score": score
                }
                mqtt.publish("factory/edge/alerts", json.dumps(payload))
                print(f"!!! TinyML LOCAL ALERT !!! Score: {score:.2f}")
            
            time.sleep(1) # Inference cycle
            
    except KeyboardInterrupt:
        print("Smart node stopped.")

if __name__ == "__main__":
    run_smart_node()
