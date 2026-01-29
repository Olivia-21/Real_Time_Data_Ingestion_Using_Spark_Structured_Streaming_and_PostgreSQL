"""
Database utilities for PostgreSQL operations.
Includes connection management, retry logic, and batch upsert functionality.
"""

import time
import psycopg2
from psycopg2 import sql, OperationalError, DatabaseError
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import os

from utils.logger import setup_logger

# Initialize logger
logger = setup_logger(
    name="db_utils",
    log_file="/app/logs/db_utils.log"
)


class DatabaseConfig:
    """Database configuration from environment variables."""
    
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "postgres")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "ecommerce_events")
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "postgres123")
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters as dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password
        }
    
    def get_jdbc_url(self) -> str:
        """Return JDBC URL for Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0
) -> Any:
    """
    Execute a function with exponential backoff retry logic.
    
    Args:
        func: Function to execute
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential backoff
    
    Returns:
        Result of the function call
    
    Raises:
        Last exception if all retries fail
    """
    last_exception = None
    delay = initial_delay
    
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except (OperationalError, DatabaseError) as e:
            last_exception = e
            if attempt < max_attempts:
                logger.warning(
                    f"Database operation failed (attempt {attempt}/{max_attempts}): {e}. "
                    f"Retrying in {delay:.1f} seconds..."
                )
                time.sleep(delay)
                delay = min(delay * exponential_base, max_delay)
            else:
                logger.error(
                    f"Database operation failed after {max_attempts} attempts: {e}"
                )
    
    raise last_exception


@contextmanager
def get_connection(config: Optional[DatabaseConfig] = None):
    """
    Context manager for database connections with automatic cleanup.
    
    Args:
        config: Database configuration. If None, uses default config.
    
    Yields:
        Database connection
    """
    if config is None:
        config = DatabaseConfig()
    
    conn = None
    try:
        conn = psycopg2.connect(**config.get_connection_params())
        yield conn
    finally:
        if conn:
            conn.close()


def batch_upsert(
    records: List[Dict[str, Any]],
    table_name: str = "user_events",
    config: Optional[DatabaseConfig] = None
) -> int:
    """
    Perform batch upsert (INSERT ... ON CONFLICT DO NOTHING) into PostgreSQL.
    Uses transaction with rollback on failure.
    
    Args:
        records: List of dictionaries containing event data
        table_name: Target table name
        config: Database configuration
    
    Returns:
        Number of rows successfully inserted
    """
    if not records:
        logger.debug("No records to insert")
        return 0
    
    if config is None:
        config = DatabaseConfig()
    
    # Define column order (must match table schema)
    columns = [
        "event_id", "user_id", "product_id", "product_name",
        "product_category", "event_type", "price", "event_timestamp"
    ]
    
    def do_upsert():
        with get_connection(config) as conn:
            with conn.cursor() as cursor:
                # Prepare values for batch insert
                values = []
                for record in records:
                    row = tuple(record.get(col) for col in columns)
                    values.append(row)
                
                # Build upsert query
                insert_query = sql.SQL("""
                    INSERT INTO {table} ({columns})
                    VALUES %s
                    ON CONFLICT (event_id) DO NOTHING
                """).format(
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(map(sql.Identifier, columns))
                )
                
                try:
                    execute_values(cursor, insert_query, values)
                    inserted_count = cursor.rowcount
                    conn.commit()
                    logger.info(
                        f"Successfully inserted {inserted_count} of {len(records)} records "
                        f"({len(records) - inserted_count} duplicates skipped)"
                    )
                    return inserted_count
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Batch insert failed, rolling back: {e}")
                    raise
    
    return retry_with_backoff(do_upsert)


def test_connection(config: Optional[DatabaseConfig] = None) -> bool:
    """
    Test database connection.
    
    Args:
        config: Database configuration
    
    Returns:
        True if connection successful, False otherwise
    """
    if config is None:
        config = DatabaseConfig()
    
    try:
        with get_connection(config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("Database connection test successful")
                    return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
    
    return False


def get_event_count(config: Optional[DatabaseConfig] = None) -> int:
    """
    Get total count of events in the database.
    
    Args:
        config: Database configuration
    
    Returns:
        Number of events in the table
    """
    if config is None:
        config = DatabaseConfig()
    
    try:
        with get_connection(config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM user_events")
                result = cursor.fetchone()
                return result[0] if result else 0
    except Exception as e:
        logger.error(f"Failed to get event count: {e}")
        return 0
