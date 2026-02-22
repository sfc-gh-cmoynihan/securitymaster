-- ============================================
-- SNOWPIPE STREAMING V2 SETUP
-- For JSON Customer Data Ingestion
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE INTERACTIVE_JSON_DB;
USE SCHEMA STREAMING;

-- ============================================
-- OPTION 1: Classic Snowpipe with Auto-Ingest
-- (Triggered by cloud storage events)
-- ============================================

-- Create the pipe for JSON ingestion
CREATE OR REPLACE PIPE CUSTOMERS_JSON_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO CUSTOMERS (
        EVENTDATE,
        COUNTERID,
        CLIENTIP,
        SEARCHENGINEID,
        SEARCHPHRASE,
        RESOLUTIONWIDTH,
        TITLE,
        ISREFRESH,
        DONTCOUNTHITS
    )
    FROM (
        SELECT 
            TO_DATE($1:EVENTDATE::STRING),
            $1:COUNTERID::NUMBER,
            $1:CLIENTIP::STRING,
            $1:SEARCHENGINEID::NUMBER,
            $1:SEARCHPHRASE::STRING,
            $1:RESOLUTIONWIDTH::NUMBER,
            $1:TITLE::STRING,
            $1:ISREFRESH::NUMBER,
            $1:DONTCOUNTHITS::NUMBER
        FROM @JSON_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT');

-- Show pipe details (includes notification channel for cloud setup)
SHOW PIPES LIKE 'CUSTOMERS_JSON_PIPE';

-- ============================================
-- OPTION 2: Snowpipe Streaming via Snowflake Connector
-- (For real-time streaming from applications)
-- ============================================

-- Create a streaming channel for the Snowflake Ingest SDK
-- This is used with the Snowflake Kafka Connector or Ingest SDK

-- First, ensure we have the right permissions
GRANT INSERT ON TABLE CUSTOMERS TO ROLE ACCOUNTADMIN;

-- Create a stream on the interactive table for CDC
CREATE OR REPLACE STREAM CUSTOMERS_STREAM ON TABLE CUSTOMERS
    APPEND_ONLY = TRUE;

-- ============================================
-- MANUAL LOADING (for testing)
-- ============================================

-- After uploading JSON files to stage, run:
-- PUT file:///path/to/json_data/*.json @JSON_STAGE AUTO_COMPRESS=FALSE;

-- Manual copy command
-- COPY INTO CUSTOMERS
-- FROM (
--     SELECT 
--         TO_DATE($1:EVENTDATE::STRING),
--         $1:COUNTERID::NUMBER,
--         $1:CLIENTIP::STRING,
--         $1:SEARCHENGINEID::NUMBER,
--         $1:SEARCHPHRASE::STRING,
--         $1:RESOLUTIONWIDTH::NUMBER,
--         $1:TITLE::STRING,
--         $1:ISREFRESH::NUMBER,
--         $1:DONTCOUNTHITS::NUMBER
--     FROM @JSON_STAGE
-- )
-- FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT');

-- ============================================
-- MONITORING QUERIES
-- ============================================

-- Check pipe status
-- SELECT SYSTEM$PIPE_STATUS('CUSTOMERS_JSON_PIPE');

-- Check copy history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'CUSTOMERS',
--     START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
-- ));

-- Check stream
-- SELECT * FROM CUSTOMERS_STREAM;

-- Verify data
-- SELECT COUNT(*) FROM CUSTOMERS;
-- SELECT * FROM CUSTOMERS LIMIT 10;
