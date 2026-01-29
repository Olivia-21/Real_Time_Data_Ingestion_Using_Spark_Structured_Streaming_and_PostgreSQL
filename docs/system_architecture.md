# System Architecture Diagram

## Data Flow Architecture

```mermaid
flowchart TB
    subgraph Docker["Docker Environment"]
        subgraph Generator["Data Generator Container"]
            DG[("data_generator.py")]
            DG -->|"Creates CSV files<br/>every 5 seconds"| CSV
        end

        subgraph SharedVolume["Shared Volume /data"]
            CSV[("CSV Files<br/>/data/incoming/")]
            CP[("Checkpoints<br/>/data/checkpoint/")]
        end

        subgraph Spark["Spark Master Container"]
            SS[("spark_streaming_to_postgres.py")]
            VAL["Validate & Clean"]
            FEB["foreachBatch Sink"]
            
            SS -->|"readStream"| CSV
            SS --> VAL
            VAL --> FEB
            SS -->|"Save progress"| CP
        end

        subgraph Database["PostgreSQL Container"]
            PG[("PostgreSQL<br/>ecommerce_events")]
            TBL["user_events table"]
            PG --- TBL
        end

        FEB -->|"psycopg2<br/>batch upsert"| PG
    end

    style Docker fill:#e1f5fe
    style Generator fill:#fff3e0
    style SharedVolume fill:#f3e5f5
    style Spark fill:#e8f5e9
    style Database fill:#fce4ec
```

## Component Interaction Sequence

```mermaid
sequenceDiagram
    participant GEN as Data Generator
    participant VOL as Shared Volume
    participant SPARK as Spark Streaming
    participant DB as PostgreSQL

    loop Every 5 seconds
        GEN->>GEN: Generate 10 events
        GEN->>VOL: Write CSV file (atomic)
    end

    loop Every 10 seconds (trigger)
        SPARK->>VOL: Check for new CSV files
        VOL-->>SPARK: Return new files
        SPARK->>SPARK: Read & parse CSV
        SPARK->>SPARK: Validate & clean data
        SPARK->>DB: Batch INSERT ON CONFLICT
        DB-->>SPARK: Return insert count
        SPARK->>VOL: Update checkpoint
    end
```

## Docker Container Architecture

```mermaid
graph LR
    subgraph Network["streaming_network"]
        subgraph PG["postgres:5432"]
            DB[(PostgreSQL 15)]
        end

        subgraph DG["data-generator"]
            GEN[Python 3.11]
        end

        subgraph SM["spark-master:4040"]
            SPARK[PySpark + Java 21]
        end
    end

    subgraph Volumes["Docker Volumes"]
        V1[(postgres_data)]
        V2[(shared_data)]
    end

    PG --- V1
    DG --- V2
    SM --- V2
    SM -->|"Port 5432"| PG

    style Network fill:#e3f2fd
    style Volumes fill:#fff8e1
```

## Data Transformation Pipeline

```mermaid
flowchart LR
    subgraph Input["Raw CSV Data"]
        A[event_id]
        B[user_id]
        C[product_id]
        D[product_name]
        E[product_category]
        F[event_type]
        G[price]
        H[event_timestamp]
    end

    subgraph Validation["Validation Rules"]
        V1["Remove NULL required fields"]
        V2["Validate event_type<br/>view or purchase"]
        V3["Validate price >= 0"]
        V4["Convert timestamp"]
        V5["Filter future timestamps"]
    end

    subgraph Output["PostgreSQL Table"]
        O1[event_id PK]
        O2[user_id]
        O3[product_id]
        O4[product_name]
        O5[product_category]
        O6[event_type]
        O7[price]
        O8[event_timestamp]
        O9[ingested_at]
    end

    Input --> Validation
    Validation --> Output

    style Input fill:#ffecb3
    style Validation fill:#c8e6c9
    style Output fill:#bbdefb
```

## Error Handling & Retry Logic

```mermaid
flowchart TD
    A[Batch Insert Attempt] --> B{Success?}
    B -->|Yes| C[Commit Transaction]
    B -->|No| D{Retries Left?}
    D -->|Yes| E[Wait with Exponential Backoff]
    E --> F[Retry Insert]
    F --> B
    D -->|No| G[Rollback Transaction]
    G --> H[Log Error]
    H --> I[Raise Exception]

    style A fill:#e3f2fd
    style C fill:#c8e6c9
    style G fill:#ffcdd2
    style I fill:#ffcdd2
```

---

## How to View These Diagrams

1. **VS Code**: Install "Markdown Preview Mermaid Support" extension
2. **GitHub**: Upload this file - GitHub renders Mermaid automatically
3. **Online**: Copy the mermaid code blocks to [mermaid.live](https://mermaid.live)
4. **Export**: Use mermaid.live to export as PNG/SVG
