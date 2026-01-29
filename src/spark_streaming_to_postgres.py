"""
Spark Structured Streaming Job for E-commerce Events.
Reads CSV files from shared volume, validates, transforms, and writes to PostgreSQL.

 Features:
- Process only new files (readStream with maxFilesPerTrigger)
- Malformed record handling (PERMISSIVE mode)
- Configurable trigger interval
- Duplicate prevention (upsert with ON CONFLICT)
- Checkpointing for fault tolerance
- Data validation and cleaning
- Retry logic for database operations
"""

import os
import sys
import time
from typing import Dict

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_timestamp,
    trim, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# Add utils to path for imports
sys.path.insert(0, "/app")

from utils.logger import setup_logger
from utils.db_utils import DatabaseConfig, batch_upsert, test_connection

# Initialize logger
logger = setup_logger(
    name="spark_streaming",
    log_file="/app/logs/spark_streaming.log"
)


class StreamingConfig:
    """Configuration manager for the streaming job."""
    
    def __init__(self, config_path: str = "/app/config/settings.yaml"):
        """Load configuration from YAML file."""
        self.config = self._load_config(config_path)
        
        # Spark settings
        self.app_name = self.config["spark"]["app_name"]
        self.trigger_interval = self.config["spark"]["trigger_interval"]
        self.checkpoint_path = self.config["spark"]["checkpoint_path"]
        self.input_path = self.config["spark"]["input_path"]
        self.max_files_per_trigger = self.config["spark"]["max_files_per_trigger"]
        
        # Validation settings
        self.min_price = self.config["validation"]["min_price"]
        self.max_future_seconds = self.config["validation"]["max_future_timestamp_seconds"]
        self.required_fields = self.config["validation"]["required_fields"]
        
        # Database config
        self.db_config = DatabaseConfig()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found at {config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """Return default configuration if file not found."""
        return {
            "spark": {
                "app_name": "EcommerceEventStreaming",
                "trigger_interval": "10 seconds",
                "checkpoint_path": "/data/checkpoint",
                "input_path": "/data/incoming",
                "max_files_per_trigger": 5
            },
            "validation": {
                "min_price": 0.0,
                "max_future_timestamp_seconds": 60,
                "required_fields": [
                    "event_id", "user_id", "product_id", "product_name",
                    "product_category", "event_type", "event_timestamp"
                ]
            }
        }


def get_event_schema() -> StructType:
    """
    Define the schema for e-commerce events to prevent schema inference issues.
    """
    return StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("product_category", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=True),  # Nullable for views
        StructField("event_timestamp", StringType(), nullable=False),
        # Column to capture malformed records
        StructField("_corrupt_record", StringType(), nullable=True)
    ])


def create_spark_session(config: StreamingConfig) -> SparkSession:
    """
    Create and configure a Spark session for streaming.
    
    Args:
        config: Streaming configuration
    
    Returns:
        Configured SparkSession
    """
    logger.info(f"Creating Spark session: {config.app_name}")
    
    # Build Spark session with optimized configurations
    spark = (SparkSession.builder
        .appName(config.app_name)
        .config("spark.jars", "/app/jars/postgresql-42.7.1.jar")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.streaming.checkpointLocation", config.checkpoint_path)
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        # Memory settings for container
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully")
    return spark


def validate_and_clean(df: DataFrame, config: StreamingConfig) -> DataFrame:
    """
    Validate and clean incoming event data.
    
    Validation rules:
    - Remove records with null required fields
    - Ensure price is non-negative (when present)
    - Validate event_type is 'view' or 'purchase'
    - Validate timestamp is not too far in the future
    - Remove corrupt records
    
    Args:
        df: Input DataFrame from CSV
        config: Streaming configuration
    
    Returns:
        Cleaned and validated DataFrame
    """
    logger.debug("Starting data validation and cleaning")
    
    # Filter out corrupt records
    cleaned_df = df.filter(col("_corrupt_record").isNull())
    
    # Drop the corrupt record column as it's no longer needed
    cleaned_df = cleaned_df.drop("_corrupt_record")
    
    # Filter out records with null required fields (except price which is nullable)
    for field in config.required_fields:
        cleaned_df = cleaned_df.filter(col(field).isNotNull())
    
    # Trim whitespace from string columns
    string_cols = ["event_id", "user_id", "product_id", "product_name", 
                   "product_category", "event_type"]
    for col_name in string_cols:
        cleaned_df = cleaned_df.withColumn(col_name, trim(col(col_name)))
    
    # Validate event_type (must be 'view' or 'purchase')
    cleaned_df = cleaned_df.filter(
        lower(col("event_type")).isin(["view", "purchase"])
    )
    
    # Normalize event_type to lowercase
    cleaned_df = cleaned_df.withColumn("event_type", lower(col("event_type")))
    
    # Validate price: must be non-negative when present
    cleaned_df = cleaned_df.filter(
        (col("price").isNull()) | (col("price") >= config.min_price)
    )
    
    # For 'view' events, ensure price is null
    cleaned_df = cleaned_df.withColumn(
        "price",
        when(col("event_type") == "view", lit(None))
        .otherwise(col("price"))
    )
    
    # Convert event_timestamp string to proper timestamp
    cleaned_df = cleaned_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Filter out records with invalid timestamps (null after conversion)
    cleaned_df = cleaned_df.filter(col("event_timestamp").isNotNull())
    
    # Filter out future timestamps (with small tolerance for clock drift)
    max_future = current_timestamp() + lit(config.max_future_seconds).cast("interval second")
    cleaned_df = cleaned_df.filter(col("event_timestamp") <= current_timestamp())
    
    return cleaned_df


def write_to_postgres(batch_df: DataFrame, batch_id: int, config: StreamingConfig):
    """
    Write a micro-batch to PostgreSQL using upsert logic.
    
    Args:
        batch_df: DataFrame containing the micro-batch data
        batch_id: Unique identifier for this batch
        config: Streaming configuration
    """
    start_time = time.time()
    
    # Collect the batch data (convert to Python list of dicts)
    try:
        records = batch_df.collect()
        
        if not records:
            logger.debug(f"Batch {batch_id}: No records to write")
            return
        
        # Convert Row objects to dictionaries
        record_dicts = []
        for row in records:
            record_dict = row.asDict()
            # Convert timestamp to string for psycopg2
            if record_dict.get("event_timestamp"):
                record_dict["event_timestamp"] = record_dict["event_timestamp"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            record_dicts.append(record_dict)
        
        # Perform batch upsert with retry logic
        inserted_count = batch_upsert(
            records=record_dicts,
            table_name="user_events",
            config=config.db_config
        )
        
        elapsed_time = time.time() - start_time
        logger.info(
            f"Batch {batch_id}: Processed {len(record_dicts)} records, "
            f"inserted {inserted_count} in {elapsed_time:.2f}s"
        )
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Failed to write to PostgreSQL: {e}")
        # Re-raise to trigger Spark's failure handling
        raise


def log_corrupt_records(df: DataFrame):
    """
    Log any corrupt records for debugging.
    This is a separate streaming query for monitoring.
    
    Args:
        df: DataFrame that may contain corrupt records
    """
    corrupt_df = df.filter(col("_corrupt_record").isNotNull())
    
    if not corrupt_df.isEmpty():
        corrupt_records = corrupt_df.select("_corrupt_record").collect()
        for record in corrupt_records:
            logger.warning(f"Corrupt record detected: {record._corrupt_record}")


def wait_for_postgres(config: StreamingConfig, max_retries: int = 30, delay: int = 2):
    """
    Wait for PostgreSQL to be available before starting the stream.
    
    Args:
        config: Streaming configuration
        max_retries: Maximum number of connection attempts
        delay: Seconds between attempts
    """
    logger.info("Waiting for PostgreSQL to be available...")
    
    for attempt in range(1, max_retries + 1):
        if test_connection(config.db_config):
            logger.info("PostgreSQL connection established")
            return
        
        logger.warning(
            f"PostgreSQL not ready (attempt {attempt}/{max_retries}), "
            f"retrying in {delay}s..."
        )
        time.sleep(delay)
    
    raise ConnectionError("Failed to connect to PostgreSQL after maximum retries")


def run_streaming_job():
    """
    Main entry point for the Spark Structured Streaming job.
    Sets up the stream, applies transformations, and writes to PostgreSQL.
    """
    logger.info("=" * 60)
    logger.info("Starting E-commerce Event Streaming Job")
    logger.info("=" * 60)
    
    # Load configuration
    config = StreamingConfig()
    
    # Wait for PostgreSQL to be ready
    wait_for_postgres(config)
    
    # Create Spark session
    spark = create_spark_session(config)
    
    try:
        # Define the input stream
        logger.info(f"Setting up file stream from: {config.input_path}")
        
        raw_stream = (spark
            .readStream
            .format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")  # Handle malformed records
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("maxFilesPerTrigger", config.max_files_per_trigger)
            .schema(get_event_schema())
            .load(config.input_path)
        )
        
        logger.info("File stream configured successfully")
        
        # Apply validation and cleaning
        validated_stream = validate_and_clean(raw_stream, config)
        
        # Write to PostgreSQL using foreachBatch
        logger.info(f"Starting streaming query with trigger: {config.trigger_interval}")
        
        query = (validated_stream
            .writeStream
            .outputMode("append")
            .foreachBatch(lambda df, id: write_to_postgres(df, id, config))
            .trigger(processingTime=config.trigger_interval)
            .option("checkpointLocation", config.checkpoint_path)
            .start()
        )
        
        logger.info("Streaming query started successfully")
        logger.info(f"Spark UI available at: http://localhost:4040")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    run_streaming_job()
