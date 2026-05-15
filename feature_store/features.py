from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float32, String

# Define the machine entity
machine = Entity(name="machine_id", join_keys=["machine_id"], description="Industrial Machine")

# Define the data source (Parquet from MinIO Data Lake)
telemetry_source = FileSource(
    path="../data/minio/industrial-data-lake/raw/telemetry/",
    timestamp_field="event_timestamp",
)

# Feature View for real-time and historical access
telemetry_fv = FeatureView(
    name="machine_telemetry_features",
    entities=[machine],
    ttl=timedelta(days=1),
    schema=[
        Field(name="vibration_rms", dtype=Float32),
        Field(name="temperature_mean", dtype=Float32),
        Field(name="power_consumption", dtype=Float32),
    ],
    online=True,
    source=telemetry_source,
    tags={"team": "maintenance"},
)
