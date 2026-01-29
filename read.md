Act as a junior Data Engineer building a production ready real-time data ingestion pipeline using Spark Structured Streaming and PostgreSQL.
Do not write demo-only or toy code. Follow streaming best practices and explain decisions in comments where necessary. Build a real-time data pipeline that simulates an e-commerce platform generating user activity events and ingests them continuously using Spark Structured Streaming, writing results into PostgreSQL safely and reliably just as described in task.md.
Data Generation (data_generator.py)

Continuously generate fake e-commerce events as CSV files

Each event must include:

event_id (unique, deterministic)
user_id
product_id
event_type (view, purchase)
price (nullable for views)
event_timestamp

spark_streaming_to_postgres.py
1. process only new files
2. Handle malformed or partial records safely 
4. Configure a reasonable trigger interval
5. Prevent duplicates insert into PostgreSQL
6. Implement error handling and logging
7. Implement checkpointing to ensure fault tolerance
8. Perform basic data cleaning and validation(handle null values correctly, cast data types correctly, non-negative prices, event timestamp validation etc)
9. Include retries and rollback logic for database operations

NB: Make the entire project production ready and follow best practices.