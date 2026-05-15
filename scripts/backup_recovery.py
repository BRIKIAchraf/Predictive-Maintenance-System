import os
import datetime
import subprocess

BACKUP_DIR = "../backups"

def backup_postgres():
    print("Backing up PostgreSQL metadata...")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{BACKUP_DIR}/postgres_backup_{timestamp}.sql"
    # Using pg_dump via docker
    subprocess.run([
        "docker", "exec", "postgres", 
        "pg_dump", "-U", "admin", "industrial_db", 
        ">", filename
    ], shell=True)
    print(f"Postgres backup saved to {filename}")

def backup_datalake():
    print("Backing up MinIO Data Lake (S3 Snapshots)...")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # In production: use 'mc mirror' or S3 replication
    # Here we just archive the data directory
    subprocess.run([
        "tar", "-czf", f"{BACKUP_DIR}/datalake_backup_{timestamp}.tar.gz", 
        "../data/minio"
    ])
    print("Data Lake backup complete.")

if __name__ == "__main__":
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    backup_postgres()
    backup_datalake()
