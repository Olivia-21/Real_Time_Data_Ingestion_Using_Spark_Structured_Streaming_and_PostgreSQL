# Test Cases

## Test Case 1: CSV Generation

**Objective**: Verify data generator produces valid CSV files

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Start data-generator container | Container runs without errors |
| 2 | Wait 10 seconds | At least 2 CSV files created |
| 3 | Check `/data/incoming/` | Files named `events_YYYYMMDD_HHMMSS_XXXXXX.csv` |
| 4 | Inspect CSV content | Header + 10 rows, 8 columns |
| 5 | Verify event_id format | UUID format (36 characters) |
| 6 | Verify price for views | NULL for `view` events |
| 7 | Verify price for purchases | Positive decimal value |

**Actual Result**: ________________

---

## Test Case 2: Spark File Detection

**Objective**: Verify Spark detects and processes new files

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Start all containers | Spark connects to stream |
| 2 | Check spark-master logs | "Streaming query started successfully" |
| 3 | Wait for trigger interval (10s) | Batch processing logged |
| 4 | Check batch logs | "Processed X records" message |

**Actual Result**: ________________

---

## Test Case 3: Data Validation

**Objective**: Verify invalid records are filtered out

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Manually create invalid CSV with negative price | File saved to /data/incoming/ |
| 2 | Wait for processing | Record rejected |
| 3 | Check PostgreSQL | Record not in database |
| 4 | Check logs | Validation message logged |

**Actual Result**: ________________

---

## Test Case 4: Duplicate Prevention

**Objective**: Verify upsert prevents duplicate event_ids

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Note current record count in PostgreSQL | Count = N |
| 2 | Copy an existing CSV file with new filename | File in /data/incoming/ |
| 3 | Wait for processing | Batch completes |
| 4 | Check PostgreSQL count | Count still = N (no increase) |
| 5 | Check logs | "X duplicates skipped" message |

**Actual Result**: ________________

---

## Test Case 5: Checkpoint Recovery

**Objective**: Verify fault tolerance via checkpoints

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Start all containers | Normal operation |
| 2 | Note last processed batch ID | Batch ID = B |
| 3 | Stop spark-master | Container stops |
| 4 | Add new CSV files while stopped | Files queued |
| 5 | Restart spark-master | Container starts |
| 6 | Check logs | Resumes from checkpoint |
| 7 | Verify new files processed | Records in PostgreSQL |

**Actual Result**: ________________

---

## Test Case 6: Database Retry Logic

**Objective**: Verify retry with exponential backoff

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Start all containers | Normal operation |
| 2 | Stop postgres container | Database unavailable |
| 3 | Check spark-master logs | Retry messages with increasing delays |
| 4 | Restart postgres container | Database available |
| 5 | Verify data written after recovery | Records in PostgreSQL |

**Actual Result**: ________________

---

## Test Summary

| Test Case | Status | Notes |
|-----------|--------|-------|
| TC1: CSV Generation | ☐ Pass / ☐ Fail | |
| TC2: File Detection | ☐ Pass / ☐ Fail | |
| TC3: Data Validation | ☐ Pass / ☐ Fail | |
| TC4: Duplicate Prevention | ☐ Pass / ☐ Fail | |
| TC5: Checkpoint Recovery | ☐ Pass / ☐ Fail | |
| TC6: Retry Logic | ☐ Pass / ☐ Fail | |
