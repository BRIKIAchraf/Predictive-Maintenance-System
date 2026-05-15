import os
import time
import random
import subprocess

SERVICES = ["kafka", "spark-streaming", "ml-inference", "mosquitto"]

def kill_service(service_name):
    print(f"!!! CHAOS !!! Killing service: {service_name}")
    subprocess.run(["docker", "stop", service_name], capture_output=True)

def start_service(service_name):
    print(f"--- RECOVERY --- Starting service: {service_name}")
    subprocess.run(["docker", "start", service_name], capture_output=True)

def run_experiment():
    print("Starting Chaos Engineering Experiment...")
    
    # 1. Kill a random service
    victim = random.choice(SERVICES)
    kill_service(victim)
    
    # 2. Wait and observe system resilience (Loki/Prometheus should detect this)
    print("Waiting 30s to observe system impact and alerting...")
    time.sleep(30)
    
    # 3. Recover
    start_service(victim)
    print("Experiment concluded. Check Grafana for resilience metrics.")

if __name__ == "__main__":
    run_experiment()
