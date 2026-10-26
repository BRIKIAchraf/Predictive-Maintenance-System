

## About

Industrial AI and MLOps reference platform covering edge telemetry, streaming features, feature stores, experiment tracking, drift monitoring and anomaly inference.
# Predictive Maintenance System

Reference architecture for an industrial AI platform combining edge telemetry, streaming features, MLOps and anomaly inference.

## Objective

Detect abnormal machine behaviour early while preserving a clear path from edge events to features, models, alerts and operator-facing state.

## End-to-end flow

~~~text
Machine simulator → MQTT edge gateway → Kafka + Avro
                                      ↓
                               Spark Streaming
                                      ↓
                    features → Feast / MLflow / MinIO
                                      ↓
                 inference + drift checks → alerts / digital twin
~~~

## Platform components

| Concern | Components |
|---|---|
| Event transport | MQTT, Mosquitto, Kafka |
| Contracts | Avro/Protobuf, Schema Registry |
| Streaming | Spark Streaming, CEP rules |
| Feature lifecycle | Feast, feature engineering services |
| Experiment tracking | MLflow |
| Drift monitoring | Evidently AI |
| Storage | MinIO/S3-compatible Parquet, PostgreSQL, Redis |
| Edge AI | SQLite offline buffer, TinyML concept |
| Distributed learning | Flower federated-learning concept |
| Platform operations | Prometheus, Loki, Tempo, OpenTelemetry |
| Security | Keycloak and Vault concepts |

## Repository structure

~~~text
infrastructure/    Docker Compose and platform services
schemas/           Event contracts and governance
edge/              Resilient gateway, simulator and TinyML node
services/          Streaming, inference, digital twin and CEP
feature_store/     Feast definitions
federated/         Flower server and client
data/              Local persistence and sample data
~~~

## Quick start

~~~bash
cd infrastructure
docker compose up -d
pip install -r edge/requirements.txt
python edge/simulator.py
python edge/gateway.py
~~~

Additional phases include feature engineering, inference, digital-twin state and federated-learning experiments.

## MLOps lifecycle

1. Capture and validate machine events.
2. Build time-windowed features.
3. Track experiments and parameters with MLflow.
4. Register or serve features through Feast.
5. Monitor drift and quality with Evidently AI.
6. Persist state and expose alerts to operators.
7. Measure false positives, missed faults and recovery behaviour.

## Status

Multi-phase industrial AI reference implementation. Some components are architectural or experimental and require integration tests, labelled fault data, security review and load testing before production deployment.

## Author

**Achraf BRIKI** · [LinkedIn](https://www.linkedin.com/in/achrafbriki/) · [Portfolio](https://achrafbriki.com)

## Delivery profile

**Status:** Prototype  
**Demo:** Local multi-service reference architecture with simulated telemetry.  
**Prerequisites:** Docker, Python and optional Spark, Feast, MLflow and Flower dependencies.  
**Validation:** Feature freshness, reproducibility, drift alerts, edge disconnection and inference fallback.  
**Performance plan:** Measure throughput, feature latency, inference p95, drift overhead and alert precision/recall.  
**Roadmap:** Labelled fixtures, CI, model promotion/rollback, security tests and model-card benchmarks.
