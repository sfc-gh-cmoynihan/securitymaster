-- ============================================
-- DEPLOY STREAMLIT APP IN SNOWFLAKE
-- ============================================

USE DATABASE SECURITY_MASTER_DB;
USE SCHEMA SECURITIES;

-- Create a stage for the Streamlit app
CREATE OR REPLACE STAGE STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT SECURITY_MASTER_APP
    ROOT_LOCATION = '@SECURITY_MASTER_DB.SECURITIES.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    TITLE = 'Security Master - S&P 500 Portfolio'
    COMMENT = 'S&P 500 Security Master with Trade Tracking and P&L Analysis';

-- Grant access to app
GRANT USAGE ON STREAMLIT SECURITY_MASTER_APP TO ROLE ACCOUNTADMIN;

-- Show the app URL
SHOW STREAMLITS LIKE 'SECURITY_MASTER_APP';
