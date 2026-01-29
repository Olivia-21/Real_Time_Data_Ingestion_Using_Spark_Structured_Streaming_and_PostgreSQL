# User Guide

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for containers
- Ports 5432 (PostgreSQL) and 4040 (Spark UI) available

## Quick Start

### 1. Clone/Navigate to Project

```powershell
cd c:\Users\OliviaDosimey\Desktop\DEM05_Real_Time_Spark_Streaming
```

### 2. Build and Start All Containers

```powershell
docker-compose up --build
```

This starts:
- PostgreSQL database (with auto-initialized schema)
- Data Generator (starts producing CSV files)
- Spark Streaming (processes files and writes to PostgreSQL)

### 3. Monitor Logs

```powershell
# All containers
docker-compose logs -f

# Specific container
docker-compose logs -f spark-master
docker-compose logs -f data-generator
docker-compose logs -f postgres
```

### 4. Verify Data in PostgreSQL

```powershell
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d ecommerce_events

# Check record count
SELECT COUNT(*) FROM user_events;

# View recent events
SELECT * FROM user_events ORDER BY ingested_at DESC LIMIT 10;

# Event type breakdown
SELECT event_type, COUNT(*) FROM user_events GROUP BY event_type;
```

### 5. Access Spark UI

Open [http://localhost:4040](http://localhost:4040) to monitor streaming job.

### 6. Stop All Containers

```powershell
docker-compose down

# To also remove volumes (clears all data)
docker-compose down -v
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port already in use | Stop conflicting service or change port in docker-compose.yml |
| Out of memory | Increase Docker memory limit |
| No data in PostgreSQL | Check spark-master logs for errors |
| Spark UI not loading | Wait 30-60 seconds for Spark to initialize |

## Configuration

Edit `config/settings.yaml` to modify:
- Batch size and generation interval
- Trigger interval for Spark
- Validation rules
- Retry settings
