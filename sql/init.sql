-- Simple PostgreSQL initialization for Week 02 Lab
-- This creates a basic staging table for landing data

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Create processed data table (for loaded CSV with structured fields)
CREATE TABLE IF NOT EXISTS staging.raw_data (
    id SERIAL PRIMARY KEY,
    crawl_timestamp TIMESTAMP,
    article_id VARCHAR(255),
    web_publication_date TIMESTAMP,
    web_title TEXT,
    body_text TEXT,
    web_url VARCHAR(500),
    section_name VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions (user already exists from Docker environment)
GRANT ALL PRIVILEGES ON SCHEMA staging TO staging_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO staging_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO staging_user;