# Performance Metrics

## Measurement Methodology

Performance metrics are collected during a 10-minute test run with default configuration:
- Batch size: 10 events
- Generation interval: 5 seconds
- Trigger interval: 10 seconds

## Key Metrics

### 1. Throughput

| Metric | Target | Actual |
|--------|--------|--------|
| Events generated per minute | ~120 | ______ |
| Events processed per minute | ~120 | ______ |
| Batches processed per minute | ~6 | ______ |

### 2. Latency

| Metric | Target | Actual |
|--------|--------|--------|
| End-to-end latency (generation → PostgreSQL) | < 20s | ______ |
| Spark batch processing time | < 5s | ______ |
| PostgreSQL write time per batch | < 1s | ______ |

### 3. Resource Utilization

| Container | CPU Usage | Memory Usage |
|-----------|-----------|--------------|
| data-generator | < 5% | < 100MB |
| spark-master | < 30% | < 1GB |
| postgres | < 10% | < 256MB |

## How to Measure

### Throughput

```sql
-- Events per minute in PostgreSQL
SELECT 
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*) AS events
FROM user_events
GROUP BY minute
ORDER BY minute DESC
LIMIT 10;
```

### Latency

```sql
-- Average end-to-end latency
SELECT 
    AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) AS avg_latency_seconds
FROM user_events
WHERE ingested_at > NOW() - INTERVAL '10 minutes';
```

### Resource Usage

```powershell
# Monitor container resource usage
docker stats --no-stream
```

## Spark UI Metrics

Access [http://localhost:4040](http://localhost:4040) for:

- **Streaming Statistics**: Input rate, processing rate, batch duration
- **Executor Metrics**: Memory, CPU, shuffle read/write
- **SQL Tab**: Query execution plans and timing

## Optimization Recommendations

| Issue | Optimization |
|-------|--------------|
| High batch processing time | Increase `spark.sql.shuffle.partitions` |
| Memory pressure | Reduce `maxFilesPerTrigger` |
| PostgreSQL bottleneck | Increase connection pool size |
| Slow writes | Use batch inserts (already implemented) |

## Baseline Performance (Expected)

Based on default configuration:
- **120 events/minute** generated
- **< 15 second** end-to-end latency
- **99.9%** data integrity (no lost events)
- **0** duplicates in PostgreSQL
