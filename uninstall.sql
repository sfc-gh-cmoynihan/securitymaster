-- ============================================
-- SECURITY MASTER EDM - UNINSTALL SCRIPT
-- ============================================
-- Author: Colm Moynihan
-- Date: February 2026
-- Version: 1.0
-- ============================================
-- WARNING: This script will permanently delete all Security Master objects!
-- Make sure to backup any data you want to keep before running.
-- ============================================

-- Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADHOC_WH;

-- ============================================
-- STEP 1: Drop Streamlit Application
-- ============================================
DROP STREAMLIT IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP;

-- ============================================
-- STEP 2: Drop External Access Integrations
-- ============================================
DROP INTEGRATION IF EXISTS OPENFIGI_ACCESS_INTEGRATION;
DROP INTEGRATION IF EXISTS YAHOO_FINANCE_INTEGRATION;

-- ============================================
-- STEP 3: Drop Stages
-- ============================================
DROP STAGE IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE;
DROP STAGE IF EXISTS SECURITY_MASTER_DB.TRADES.EQUITY_ORDERS;
DROP STAGE IF EXISTS SECURITY_MASTER_DB.TRADES.BOND_ORDERS;

-- ============================================
-- STEP 4: Drop Functions (UDFs)
-- ============================================
DROP FUNCTION IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.LOOKUP_ISIN_EXTERNAL(VARCHAR);
DROP FUNCTION IF EXISTS SECURITY_MASTER_DB.TRADES.GET_STOCK_PRICE(VARCHAR);

-- ============================================
-- STEP 5: Drop Streams
-- ============================================
DROP STREAM IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_CHANGES;

-- ============================================
-- STEP 6: Drop Network Rules
-- ============================================
DROP NETWORK RULE IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.OPENFIGI_NETWORK_RULE;
DROP NETWORK RULE IF EXISTS SECURITY_MASTER_DB.TRADES.YAHOO_FINANCE_NETWORK_RULE;

-- ============================================
-- STEP 7: Drop Sequences
-- ============================================
DROP SEQUENCE IF EXISTS SECURITY_MASTER_DB.TRADES.ORDER_ID_SEQ;

-- ============================================
-- STEP 8: Drop Tables (Hybrid Tables first)
-- ============================================
-- Hybrid Tables
DROP TABLE IF EXISTS SECURITY_MASTER_DB.TRADES.EQUITY_TRADES;
DROP TABLE IF EXISTS SECURITY_MASTER_DB.TRADES.BOND_TRADES;

-- Standard Tables
DROP TABLE IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE;
DROP TABLE IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY;
DROP TABLE IF EXISTS SECURITY_MASTER_DB.SECURITIES.SP500;
DROP TABLE IF EXISTS SECURITY_MASTER_DB.SECURITIES.NYSE_LISTED;
DROP TABLE IF EXISTS SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS;

-- ============================================
-- STEP 9: Drop Schemas
-- ============================================
DROP SCHEMA IF EXISTS SECURITY_MASTER_DB.GOLDEN_RECORD;
DROP SCHEMA IF EXISTS SECURITY_MASTER_DB.TRADES;
DROP SCHEMA IF EXISTS SECURITY_MASTER_DB.SECURITIES;
DROP SCHEMA IF EXISTS SECURITY_MASTER_DB.FIXED_INCOME;
DROP SCHEMA IF EXISTS SECURITY_MASTER_DB.RAW_DATA;

-- ============================================
-- STEP 10: Drop Database
-- ============================================
DROP DATABASE IF EXISTS SECURITY_MASTER_DB;

-- ============================================
-- VERIFICATION
-- ============================================
-- Run these queries to verify cleanup:
-- SHOW DATABASES LIKE 'SECURITY_MASTER_DB';
-- SHOW INTEGRATIONS LIKE '%OPENFIGI%';
-- SHOW INTEGRATIONS LIKE '%YAHOO%';

SELECT 'Security Master EDM uninstall complete.' AS STATUS;
