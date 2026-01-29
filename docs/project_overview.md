# Real-Time E-commerce Event Streaming Pipeline

## Overview

This project implements a production-ready real-time data pipeline that:

1. **Generates** fake e-commerce user activity events (views, purchases)
2. **Streams** events using Apache Spark Structured Streaming
3. **Stores** validated data in PostgreSQL with duplicate prevention

## Architecture

```
┌─────────────────┐      ┌──────────────┐      ┌──────────────┐
│ Data Generator  │─────▶│ Shared Volume│─────▶│ Spark Master │
│   (Python)      │ CSV  │ /data/incoming│      │  (PySpark)   │
└─────────────────┘      └──────────────┘      └──────┬───────┘
                                                       │ psycopg2
                                                       ▼
                                               ┌──────────────┐
                                               │  PostgreSQL  │
                                               │ user_events  │
                                               └──────────────┘
```

## Components

| Component | Container | Description |
|-----------|-----------|-------------|
| Data Generator | `data-generator` | Produces CSV files with fake events every 5 seconds |
| Spark Streaming | `spark-master` | Reads CSVs, validates, transforms, writes to PostgreSQL |
| Database | `postgres` | Stores validated events with duplicate prevention |

## Event Schema

| Field | Type | Description |
|-------|------|-------------|
| event_id | VARCHAR(36) | Deterministic UUID (PK) |
| user_id | VARCHAR(20) | User identifier |
| product_id | VARCHAR(20) | Product identifier |
| product_name | VARCHAR(255) | Product name |
| product_category | VARCHAR(100) | Category (Electronics, Clothing, etc.) |
| event_type | VARCHAR(20) | `view` or `purchase` |
| price | DECIMAL(10,2) | Purchase price (NULL for views) |
| event_timestamp | TIMESTAMP | When event occurred |
| ingested_at | TIMESTAMP | When record was written |

## Key Features

- **Fault Tolerance**: Checkpointing enables recovery from failures
- **Duplicate Prevention**: Upsert using `ON CONFLICT DO NOTHING`
- **Data Validation**: Null handling, type casting, business rule validation
- **Retry Logic**: Exponential backoff for database operations
- **Graceful Shutdown**: Signal handling for clean termination

## Data Flow

1. Generator creates CSV batch → writes to `/data/incoming/`
2. Spark detects new files via streaming source
3. Records are validated and cleaned
4. Valid records upserted to PostgreSQL
5. Checkpoint updated for fault tolerance
