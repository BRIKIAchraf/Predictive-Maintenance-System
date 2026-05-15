# 🚀 Smart Industrial AI Platform

Welcome to your Enterprise-Grade Industrial AI Platform. This project implements a cutting-edge "Staff Engineer" level architecture for Industry 5.0.

## 🏗️ Enterprise Architecture Stack
- **Messaging:** Kafka (Backbone) + Mosquitto (MQTT Edge)
- **Data Governance:** Confluent Schema Registry (Avro/Protobuf)
- **Observability:** Loki (Logs), Tempo (Traces), Prometheus (Metrics), OpenTelemetry
- **MLOps Excellence:** **Feast** (Feature Store), **MLflow** (Registry), **Evidently AI** (Drift Detection)
- **Enterprise Tier:** **Keycloak** (IAM), **HashiCorp Vault** (Secrets), **PostgreSQL** (Metadata)
- **Real-time Engine:** Spark Streaming + **CEP Rule Engine**
- **Data Lake:** MinIO (S3 Compatible) + Parquet Historization
- **Edge AI:** TinyML + Resilient Gateway (SQLite Offline Buffering)
- **Distributed AI:** Federated Learning (Flower)

## 📂 Project Structure
```bash
├── infrastructure/    # Docker Compose (Kafka, Loki, Keycloak, etc.)
├── schemas/           # Avro Data Governance
├── edge/              # Resilient Gateway, TinyML Node, Simulator
├── services/          # Spark, ML Inference, Digital Twin, CEP, Drift
├── feature_store/     # Feast definitions
├── federated/         # Federated Learning
└── data/              # Local persistence (Postgres, Redis, Buffer)
```

## 🛠️ How to Run

### 1. Infrastructure (Prerequisite: Docker)
```bash
cd infrastructure
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r edge/requirements.txt
# Add: flwr, pyspark if needed
```

### 3. Start the Data Flow (Phase 1)
- **Run Simulator:** `python edge/simulator.py` (Generates raw JSON MQTT)
- **Run Gateway:** `python edge/gateway.py` (Bridges to Kafka with Avro)

### 4. Start Real-time Processing (Phase 3)
- **Run Feature Engineering:** `python services/feature_engineering.py` (Spark)

### 5. Start Intelligent Services (Phase 4 & 5)
- **Run ML Inference:** `python services/ml_inference.py` (Detects anomalies)
- **Run Digital Twin:** `python services/digital_twin.py` (Virtual state)

### 6. Advanced AI (Phase 6 & 7)
- **Run TinyML Node:** `python edge/smart_node.py` (Local inference)
- **Run FL Server:** `python federated/server.py`
- **Run FL Client:** `python federated/client.py`

## 📊 Verification
- **Kafka UI:** [http://localhost:9000](http://localhost:9000)
- **MinIO UI:** [http://localhost:9006](http://localhost:9006)
- **Digital Twin API:** [http://localhost:8001/dashboard](http://localhost:8001/dashboard)
- **ML Health:** [http://localhost:8000/health](http://localhost:8000/health)
