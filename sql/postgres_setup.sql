-- PostgreSQL Setup Script for E-commerce Event Streaming
-- This script initializes the database and creates the required table

-- Create the user_events table with proper schema
CREATE TABLE IF NOT EXISTS user_events (
    -- Primary key using event_id for uniqueness
    event_id VARCHAR(36) PRIMARY KEY,
    
    -- User identification
    user_id VARCHAR(20) NOT NULL,
    
    -- Product information
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    
    -- Event details
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'purchase')),
    price DECIMAL(10, 2),  -- Nullable for 'view' events
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Metadata for tracking ingestion
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint: price must be non-negative when provided
    CONSTRAINT price_non_negative CHECK (price IS NULL OR price >= 0)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_product_id ON user_events(product_id);
CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_events_event_timestamp ON user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_user_events_product_category ON user_events(product_category);

-- Create a composite index for time-based queries with filtering
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp_type 
    ON user_events(event_timestamp, event_type);

-- Grant permissions (adjust as needed for your security requirements)
-- GRANT ALL PRIVILEGES ON TABLE user_events TO your_app_user;

COMMENT ON TABLE user_events IS 'Stores e-commerce user activity events from real-time streaming pipeline';
COMMENT ON COLUMN user_events.event_id IS 'Unique deterministic ID generated from timestamp + user_id + product_id';
COMMENT ON COLUMN user_events.price IS 'Purchase price in dollars, NULL for view events';
COMMENT ON COLUMN user_events.ingested_at IS 'Timestamp when record was written by Spark streaming job';
