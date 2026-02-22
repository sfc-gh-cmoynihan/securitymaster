-- ============================================
-- INTERACTIVE TABLE STREAMING SETUP
-- Using Snowflake Ingest SDK
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE INTERACTIVE_JSON_DB;
USE SCHEMA STREAMING;

-- ============================================
-- CREATE INTERACTIVE TABLE
-- ============================================

-- Interactive Tables are optimized for real-time analytics
-- with sub-second query latency. They can ONLY be populated
-- via the Snowflake Ingest SDK (not SQL INSERT/COPY).

CREATE OR REPLACE INTERACTIVE TABLE CUSTOMERS CLUSTER BY (CLIENTIP) (
    EVENTDATE DATE,
    COUNTERID NUMBER(38,0),
    CLIENTIP VARCHAR(16777216),
    SEARCHENGINEID NUMBER(38,0),
    SEARCHPHRASE VARCHAR(16777216),
    RESOLUTIONWIDTH NUMBER(38,0),
    TITLE VARCHAR(16777216),
    ISREFRESH NUMBER(38,0),
    DONTCOUNTHITS NUMBER(38,0)
);

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Check record count
SELECT 'Interactive table records: ' || COUNT(*) AS status FROM CUSTOMERS;

-- Sample data
SELECT * FROM CUSTOMERS LIMIT 10;

-- Traffic by UK City
SELECT 
    CASE 
        WHEN CLIENTIP LIKE '185.86.%' OR CLIENTIP LIKE '31.52.%' THEN 'London'
        WHEN CLIENTIP LIKE '81.105.%' OR CLIENTIP LIKE '82.132.%' THEN 'Manchester'
        WHEN CLIENTIP LIKE '92.233.%' OR CLIENTIP LIKE '86.149.%' THEN 'Birmingham'
        WHEN CLIENTIP LIKE '90.216.%' OR CLIENTIP LIKE '86.146.%' THEN 'Leeds'
        WHEN CLIENTIP LIKE '92.238.%' OR CLIENTIP LIKE '86.159.%' THEN 'Glasgow'
        ELSE 'Other UK'
    END AS CITY,
    COUNT(*) AS VISITORS
FROM CUSTOMERS
GROUP BY 1
ORDER BY 2 DESC;

-- ============================================
-- NOTES
-- ============================================
-- 
-- Interactive Tables can ONLY be populated via:
--   1. Snowflake Ingest SDK (Python)
--   2. Snowflake Kafka Connector
--   3. Snowflake Spark Connector (streaming mode)
--
-- These do NOT work with Interactive Tables:
--   - SQL INSERT statements
--   - COPY INTO commands
--   - Regular Snowpipe
--
-- See snowpipe_streaming_ingest.py for Python streaming example.
-- ============================================
