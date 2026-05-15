import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset
from confluent_kafka import Consumer
import json
import time

# Configuration
KAFKA_BROKER = "localhost:29092"
FEATURE_TOPIC = "factory.features.realtime"

# Reference Data (Mocking a baseline dataset)
reference_data = pd.DataFrame({
    'vibration_rms': [0.5, 0.6, 0.4, 0.55, 0.45] * 10,
    'temperature_mean': [65, 66, 64, 67, 63] * 10
})

current_data_buffer = []

def run_drift_analysis():
    global current_data_buffer
    if len(current_data_buffer) < 20: return
    
    current_df = pd.DataFrame(current_data_buffer)
    
    # Create Evidently Report
    report = Report(metrics=[
        DataDriftPreset(),
        TargetDriftPreset()
    ])
    
    report.run(reference_data=reference_data, current_data=current_df)
    
    # Check for drift
    report_dict = report.as_dict()
    drift_detected = report_dict['metrics'][0]['result']['dataset_drift']
    
    if drift_detected:
        print("!!! WARNING: DATA DRIFT DETECTED !!! Re-training of models might be required.")
        # In real setup: trigger Airflow DAG for retraining
        
    # Clear buffer after analysis
    current_data_buffer = []

def run_service():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'drift-detection-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([FEATURE_TOPIC])
    
    print("Drift Detection Service started. Monitoring for Concept Drift...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            
            data = json.loads(msg.value().decode())
            current_data_buffer.append({
                'vibration_rms': data['vibration_rms'],
                'temperature_mean': data['temperature_mean']
            })
            
            if len(current_data_buffer) >= 50:
                run_drift_analysis()
                
    except Exception as e:
        print(f"Drift service error: {e}")

if __name__ == "__main__":
    run_service()
