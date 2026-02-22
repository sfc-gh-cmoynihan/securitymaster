-- ============================================
-- SECURITY MASTER EDM - INSTALLATION SCRIPT
-- ============================================
-- Author: Colm Moynihan
-- Date: February 2026
-- Version: 1.2
-- ============================================

-- This script installs the complete Security Master EDM application
-- Run each section in order

-- ============================================
-- STEP 1: Create Database Structure
-- ============================================
-- Execute: sql/setup_security_master.sql
-- Creates: SECURITY_MASTER_DB database with all schemas

-- ============================================
-- STEP 2: Create Golden Record Tables
-- ============================================
-- Execute: sql/setup_golden_record_tables.sql
-- Creates: SECURITY_MASTER_REFERENCE, SECURITY_MASTER_HISTORY tables

-- ============================================
-- STEP 3: Load Sample Trade Data
-- ============================================
-- Execute: sql/create_trades.sql
-- Creates: Sample equity and bond trades

-- ============================================
-- STEP 4: Setup External API Integrations
-- ============================================

-- 4a. OpenFIGI API (ISIN Lookup)
CREATE OR REPLACE NETWORK RULE SECURITY_MASTER_DB.GOLDEN_RECORD.OPENFIGI_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.openfigi.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION OPENFIGI_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (SECURITY_MASTER_DB.GOLDEN_RECORD.OPENFIGI_NETWORK_RULE)
    ENABLED = TRUE;

-- 4b. Yahoo Finance API (Live Stock Prices)
CREATE OR REPLACE NETWORK RULE SECURITY_MASTER_DB.TRADES.YAHOO_FINANCE_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('query1.finance.yahoo.com:443', 'query2.finance.yahoo.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION YAHOO_FINANCE_INTEGRATION
    ALLOWED_NETWORK_RULES = (SECURITY_MASTER_DB.TRADES.YAHOO_FINANCE_NETWORK_RULE)
    ENABLED = TRUE;

-- ============================================
-- STEP 5: Create UDFs
-- ============================================

-- 5a. ISIN Lookup Function
CREATE OR REPLACE FUNCTION SECURITY_MASTER_DB.GOLDEN_RECORD.LOOKUP_ISIN_EXTERNAL(isin VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (OPENFIGI_ACCESS_INTEGRATION)
HANDLER = 'lookup_isin'
AS
$$
import requests
import json

def lookup_isin(isin):
    try:
        url = "https://api.openfigi.com/v3/mapping"
        headers = {"Content-Type": "application/json"}
        payload = [{"idType": "ID_ISIN", "idValue": isin}]
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0 and 'data' in data[0]:
                results = data[0]['data']
                if results:
                    first = results[0]
                    return {
                        'success': True,
                        'name': first.get('name', ''),
                        'ticker': first.get('ticker', ''),
                        'exchange': first.get('exchCode', ''),
                        'security_type': first.get('securityType', ''),
                        'figi': first.get('figi', '')
                    }
            return {'success': False, 'error': 'No results found'}
        return {'success': False, 'error': f'API error {response.status_code}'}
    except Exception as e:
        return {'success': False, 'error': str(e)}
$$;

-- 5b. Live Stock Price Function
CREATE OR REPLACE FUNCTION SECURITY_MASTER_DB.TRADES.GET_STOCK_PRICE(SYMBOL VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (YAHOO_FINANCE_INTEGRATION)
HANDLER = 'get_price'
AS
$$
import requests

def get_price(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            result = data.get('chart', {}).get('result', [{}])[0]
            meta = result.get('meta', {})
            return {
                'symbol': symbol,
                'price': meta.get('regularMarketPrice'),
                'previous_close': meta.get('previousClose'),
                'market_state': meta.get('marketState'),
                'exchange': meta.get('exchangeName')
            }
        return {'error': f'API error {response.status_code}'}
    except Exception as e:
        return {'error': str(e)}
$$;

-- ============================================
-- STEP 6: Create FIX Stage for FIXML Messages
-- ============================================
CREATE OR REPLACE STAGE SECURITY_MASTER_DB.TRADES.FIX_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- ============================================
-- STEP 7: Deploy Streamlit Application
-- ============================================
CREATE OR REPLACE STAGE SECURITY_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Upload streamlit/streamlit_app.py to the stage:
-- PUT file:///path/to/streamlit/streamlit_app.py @SECURITY_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CREATE OR REPLACE STREAMLIT SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP
    ROOT_LOCATION = '@SECURITY_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'ADHOC_WH'
    TITLE = 'Security Master EDM';

-- ============================================
-- INSTALLATION COMPLETE
-- ============================================
-- Access the app at:
-- https://app.snowflake.com/<account>/#/streamlit-apps/SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP
