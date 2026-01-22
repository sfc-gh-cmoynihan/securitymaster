-- ============================================
-- SNOWPIPE STREAMING V2 SETUP
-- For JSON Customer Data -> Interactive Table
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE INTERACTIVE_JSON_DB;
USE SCHEMA STREAMING;

-- ============================================
-- STEP 1: Create a staging table for Snowpipe
-- (Interactive tables don't support direct DML from pipes)
-- ============================================

CREATE OR REPLACE TABLE CUSTOMERS_STAGING (
    EVENTDATE DATE,
    COUNTERID NUMBER(38,0),
    CLIENTIP VARCHAR(16777216),
    SEARCHENGINEID NUMBER(38,0),
    SEARCHPHRASE VARCHAR(16777216),
    RESOLUTIONWIDTH NUMBER(38,0),
    TITLE VARCHAR(16777216),
    ISREFRESH NUMBER(38,0),
    DONTCOUNTHITS NUMBER(38,0),
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STEP 2: Create Snowpipe to load into staging
-- ============================================

CREATE OR REPLACE PIPE CUSTOMERS_JSON_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO CUSTOMERS_STAGING (
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

-- Show pipe notification channel (for cloud storage integration)
SHOW PIPES LIKE 'CUSTOMERS_JSON_PIPE';

-- ============================================
-- STEP 3: Create a stream on staging table
-- ============================================

CREATE OR REPLACE STREAM CUSTOMERS_STAGING_STREAM 
    ON TABLE CUSTOMERS_STAGING
    APPEND_ONLY = TRUE;

-- ============================================
-- STEP 4: Create task to move data to Interactive Table
-- Runs every minute to sync new data
-- ============================================

CREATE OR REPLACE TASK SYNC_CUSTOMERS_TO_INTERACTIVE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMERS_STAGING_STREAM')
    AS
    INSERT INTO CUSTOMERS (
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
    SELECT 
        EVENTDATE,
        COUNTERID,
        CLIENTIP,
        SEARCHENGINEID,
        SEARCHPHRASE,
        RESOLUTIONWIDTH,
        TITLE,
        ISREFRESH,
        DONTCOUNTHITS
    FROM CUSTOMERS_STAGING_STREAM;

-- Enable the task
ALTER TASK SYNC_CUSTOMERS_TO_INTERACTIVE RESUME;

-- ============================================
-- STEP 5: Manual initial load from stage
-- ============================================

-- Load data from JSON files already in stage
COPY INTO CUSTOMERS_STAGING (
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

-- Manually trigger the sync to interactive table
INSERT INTO CUSTOMERS (
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
SELECT 
    EVENTDATE,
    COUNTERID,
    CLIENTIP,
    SEARCHENGINEID,
    SEARCHPHRASE,
    RESOLUTIONWIDTH,
    TITLE,
    ISREFRESH,
    DONTCOUNTHITS
FROM CUSTOMERS_STAGING;

-- ============================================
-- VERIFICATION
-- ============================================

-- Check staging table
SELECT 'Staging table records: ' || COUNT(*) AS status FROM CUSTOMERS_STAGING;

-- Check interactive table
SELECT 'Interactive table records: ' || COUNT(*) AS status FROM CUSTOMERS;

-- Check task status
SHOW TASKS LIKE 'SYNC_CUSTOMERS_TO_INTERACTIVE';

-- Sample data
SELECT * FROM CUSTOMERS LIMIT 10;
