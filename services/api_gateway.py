from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import OAuth2PasswordBearer
from typing import List
import requests
import os

app = FastAPI(title="Industrial Platform API Gateway")

# Configuration
TWIN_SERVICE_URL = os.getenv('TWIN_SERVICE_URL', 'http://localhost:8001')
ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')
KEYCLOAK_URL = os.getenv('KEYCLOAK_URL', 'http://localhost:8080')

# OAuth2 / Keycloak Setup (Mocked for demo)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    # In production: verify JWT with Keycloak
    if not token:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    return {"user": "admin", "role": "maintenance_engineer"}

@app.get("/api/v1/health")
def platform_health():
    return {"status": "operational", "version": "1.0.0-enterprise"}

@app.get("/api/v1/machines", dependencies=[Depends(get_current_user)])
def get_all_machines():
    """Proxies request to the Digital Twin service."""
    try:
        response = requests.get(f"{TWIN_SERVICE_URL}/dashboard")
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail="Digital Twin service unavailable")

@app.get("/api/v1/ml/metrics", dependencies=[Depends(get_current_user)])
def get_ml_metrics():
    """Returns precision, recall, and f1 scores (Simulated)."""
    # In production: pull from MLflow or Prometheus
    return {
        "precision": 0.94,
        "recall": 0.88,
        "f1_score": 0.91,
        "auc_roc": 0.96,
        "class_imbalance_ratio": "1:100",
        "last_retraining": "2026-05-14T10:00:00Z"
    }

@app.get("/api/v1/alerts", dependencies=[Depends(get_current_user)])
def get_recent_alerts():
    # In production: query Postgres
    return [
        {"id": 1, "machine": "MOTOR_01", "severity": "CRITICAL", "type": "Z-Score Violation"},
        {"id": 2, "machine": "PUMP_04", "severity": "WARNING", "type": "Thermal Rise"}
    ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
