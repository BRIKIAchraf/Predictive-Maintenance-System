from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import time
import sqlalchemy
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

app = FastAPI(title="Industrial Device Management Service")

# Database Configuration
DATABASE_URL = "postgresql://admin:password123@localhost:5432/industrial_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- DATABASE MODELS ---
class Device(Base):
    __tablename__ = "devices"
    id = Column(String, primary_key=True, index=True)
    type = Column(String)
    factory_id = Column(String)
    firmware_version = Column(String)
    status = Column(String, default="OFFLINE")
    last_heartbeat = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(bind=engine)

# --- SCHEMAS ---
class DeviceCreate(BaseModel):
    id: str
    type: str
    factory_id: str
    firmware_version: str

# --- ENDPOINTS ---
@app.post("/devices/", response_model=DeviceCreate)
def register_device(device: DeviceCreate):
    db = SessionLocal()
    db_device = Device(**device.dict())
    db.add(db_device)
    db.commit()
    db.refresh(db_device)
    return db_device

@app.post("/devices/{device_id}/heartbeat")
def device_heartbeat(device_id: str):
    db = SessionLocal()
    db_device = db.query(Device).filter(Device.id == device_id).first()
    if not db_device:
        raise HTTPException(status_code=404, detail="Device not found")
    
    db_device.last_heartbeat = datetime.datetime.utcnow()
    db_device.status = "ONLINE"
    db.commit()
    return {"status": "success", "timestamp": db_device.last_heartbeat}

@app.get("/devices/", response_model=List[DeviceCreate])
def list_devices():
    db = SessionLocal()
    return db.query(Device).all()

@app.post("/devices/{device_id}/ota-update")
def trigger_ota(device_id: str, version: str):
    """Mocks an Over-The-Air firmware update trigger."""
    db = SessionLocal()
    db_device = db.query(Device).filter(Device.id == device_id).first()
    if not db_device:
        raise HTTPException(status_code=404, detail="Device not found")
    
    db_device.firmware_version = version
    db.commit()
    print(f"!!! OTA UPDATE TRIGGERED !!! Device {device_id} updating to {version}")
    return {"status": "updating", "target_version": version}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
