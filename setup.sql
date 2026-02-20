-- ============================================
-- COLM_DB COMPLETE SETUP SCRIPT
-- Securities Master Database Installation
-- ============================================
-- 
-- This script creates all database objects including:
-- - Database and Schemas
-- - Tables (SP500, NYSE_SECURITIES, CORPORATE_BONDS, SECURITY_MASTER_REFERENCE, SECURITY_MASTER_HISTORY)
-- - Sequence (GSID_SEQ)
-- - External Function (LOOKUP_ISIN_EXTERNAL)
-- - Stages (SECURITY_DATA_STAGE, EXPORT)
-- - File Formats
-- - Sample Data
--
-- Usage: Execute this script as ACCOUNTADMIN or a role with CREATE DATABASE privileges
-- ============================================

USE ROLE ACCOUNTADMIN;

-- ============================================
-- DATABASE AND SCHEMAS
-- ============================================

CREATE DATABASE IF NOT EXISTS COLM_DB;
USE DATABASE COLM_DB;

CREATE SCHEMA IF NOT EXISTS SECURITIES;
CREATE SCHEMA IF NOT EXISTS FIXED_INCOME;
CREATE SCHEMA IF NOT EXISTS GOLDEN_RECORD;

-- ============================================
-- FILE FORMATS
-- ============================================

USE SCHEMA SECURITIES;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('');

CREATE OR REPLACE FILE FORMAT JSON_FORMAT 
    TYPE = 'JSON' 
    STRIP_OUTER_ARRAY = TRUE;

-- ============================================
-- STAGES
-- ============================================

CREATE OR REPLACE STAGE SECURITY_DATA_STAGE
    COMMENT = 'Stage for loading security master data files';

CREATE OR REPLACE STAGE EXPORT
    COMMENT = 'Stage for exporting security master data';

-- ============================================
-- SEQUENCE FOR GLOBAL SECURITY IDS
-- ============================================

USE SCHEMA GOLDEN_RECORD;

CREATE OR REPLACE SEQUENCE GSID_SEQ 
    START = 100001 
    INCREMENT = 1
    COMMENT = 'Sequence for generating unique Global Security IDs';

-- ============================================
-- EXTERNAL FUNCTION: LOOKUP_ISIN_EXTERNAL
-- Calls OpenFIGI API to lookup security by ISIN
-- ============================================
-- Note: This requires an API Integration to be set up first.
-- Uncomment and modify the following if you have an API Integration configured:

/*
CREATE OR REPLACE EXTERNAL FUNCTION LOOKUP_ISIN_EXTERNAL(isin VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = openfigi_api_integration
    AS 'https://api.openfigi.com/v3/mapping'
    COMMENT = 'Lookup security information from OpenFIGI API by ISIN';
*/

-- Alternative: Create as a stub function for testing without external API
CREATE OR REPLACE FUNCTION LOOKUP_ISIN_EXTERNAL(isin VARCHAR)
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    COMMENT = 'Stub function for ISIN lookup - replace with external function when API integration is configured'
AS
$$
    return {
        "success": true,
        "isin": ISIN,
        "name": "Sample Security",
        "ticker": "SAMPLE",
        "exchange": "NYSE",
        "security_type": "Common Stock",
        "market_sector": "Equity",
        "figi": "BBG000000000",
        "message": "This is a stub function. Configure API Integration for live data."
    };
$$;

-- ============================================
-- TABLE: SP500
-- S&P 500 Companies Reference Data
-- ============================================

USE SCHEMA SECURITIES;

CREATE OR REPLACE TABLE SP500 (
    SYMBOL VARCHAR(10) PRIMARY KEY,
    SECURITY_NAME VARCHAR(200),
    GICS_SECTOR VARCHAR(100),
    GICS_SUB_INDUSTRY VARCHAR(100),
    HEADQUARTERS VARCHAR(200),
    DATE_ADDED DATE,
    CIK VARCHAR(20),
    FOUNDED VARCHAR(20)
);

COMMENT ON TABLE SP500 IS 'S&P 500 constituent companies with sector classification';

-- ============================================
-- TABLE: NYSE_SECURITIES
-- NYSE Listed Securities
-- ============================================

CREATE OR REPLACE TABLE NYSE_SECURITIES (
    SYMBOL VARCHAR(20) NOT NULL,
    COMPANY_NAME VARCHAR(500),
    ISIN VARCHAR(12),
    EXCHANGE VARCHAR(10) DEFAULT 'NYSE',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE NYSE_SECURITIES IS 'NYSE listed securities with ISIN identifiers';

-- ============================================
-- TABLE: CORPORATE_BONDS
-- Fixed Income Securities
-- ============================================

USE SCHEMA FIXED_INCOME;

CREATE OR REPLACE TABLE CORPORATE_BONDS (
    BOND_ID INTEGER PRIMARY KEY,
    CUSIP VARCHAR(9) NOT NULL,
    ISIN VARCHAR(12),
    FIGI VARCHAR(12),
    TICKER VARCHAR(10),
    ISSUER_NAME VARCHAR(200) NOT NULL,
    ISSUE_DATE DATE,
    MATURITY_DATE DATE,
    COUPON_RATE DECIMAL(6,3),
    COUPON_FREQUENCY VARCHAR(20),
    CURRENT_YIELD DECIMAL(6,3),
    CREDIT_RATING VARCHAR(5),
    PAR_VALUE DECIMAL(18,2),
    CURRENCY VARCHAR(3) DEFAULT 'USD',
    BOND_TYPE VARCHAR(50),
    CALLABLE BOOLEAN,
    SECTOR VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE CORPORATE_BONDS IS 'Corporate bond securities with pricing and credit information';

-- ============================================
-- TABLE: SECURITY_MASTER_REFERENCE
-- Golden Record for Securities
-- ============================================

USE SCHEMA GOLDEN_RECORD;

CREATE OR REPLACE TABLE SECURITY_MASTER_REFERENCE (
    GLOBAL_SECURITY_ID VARCHAR(50) PRIMARY KEY,
    ISSUER VARCHAR(500) NOT NULL,
    ASSET_CLASS VARCHAR(50) NOT NULL,
    PRIMARY_TICKER VARCHAR(20),
    PRIMARY_EXCHANGE VARCHAR(50),
    ISIN VARCHAR(12),
    CUSIP VARCHAR(9),
    SEDOL VARCHAR(7),
    CURRENCY VARCHAR(3) NOT NULL,
    STATUS VARCHAR(20) DEFAULT 'ACTIVE',
    GOLDEN_SOURCE VARCHAR(200),
    LAST_VALIDATED TIMESTAMP_NTZ,
    LINEAGE_ID VARCHAR(100),
    CREATED_BY VARCHAR(100),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_MODIFIED_BY VARCHAR(100),
    LAST_MODIFIED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE SECURITY_MASTER_REFERENCE IS 'Golden record reference table - the single source of truth for all securities';

-- ============================================
-- TABLE: SECURITY_MASTER_HISTORY
-- Full Audit Trail
-- ============================================

CREATE OR REPLACE TABLE SECURITY_MASTER_HISTORY (
    HISTORY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    GLOBAL_SECURITY_ID VARCHAR(50) NOT NULL,
    ACTION VARCHAR(20) NOT NULL,
    ISSUER_BEFORE VARCHAR(500),
    ISSUER_AFTER VARCHAR(500),
    ASSET_CLASS_BEFORE VARCHAR(50),
    ASSET_CLASS_AFTER VARCHAR(50),
    PRIMARY_TICKER_BEFORE VARCHAR(20),
    PRIMARY_TICKER_AFTER VARCHAR(20),
    PRIMARY_EXCHANGE_BEFORE VARCHAR(50),
    PRIMARY_EXCHANGE_AFTER VARCHAR(50),
    ISIN_BEFORE VARCHAR(12),
    ISIN_AFTER VARCHAR(12),
    CUSIP_BEFORE VARCHAR(9),
    CUSIP_AFTER VARCHAR(9),
    SEDOL_BEFORE VARCHAR(7),
    SEDOL_AFTER VARCHAR(7),
    CURRENCY_BEFORE VARCHAR(3),
    CURRENCY_AFTER VARCHAR(3),
    STATUS_BEFORE VARCHAR(20),
    STATUS_AFTER VARCHAR(20),
    EDIT_REASON VARCHAR(1000),
    CHANGED_BY VARCHAR(100),
    CHANGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_SYSTEM VARCHAR(100),
    LINEAGE_ID VARCHAR(100),
    LINEAGE_PARENT_ID VARCHAR(100),
    LINEAGE_PATH VARCHAR(1000)
);

COMMENT ON TABLE SECURITY_MASTER_HISTORY IS 'Complete audit trail of all security master changes with before/after state';

-- ============================================
-- SAMPLE DATA: SP500 (Top 50 companies)
-- ============================================

USE SCHEMA SECURITIES;

INSERT INTO SP500 (SYMBOL, SECURITY_NAME, GICS_SECTOR, GICS_SUB_INDUSTRY, HEADQUARTERS, CIK, FOUNDED) VALUES
('AAPL', 'Apple Inc.', 'Information Technology', 'Technology Hardware', 'Cupertino, California', '0000320193', '1976'),
('MSFT', 'Microsoft', 'Information Technology', 'Systems Software', 'Redmond, Washington', '0000789019', '1975'),
('AMZN', 'Amazon', 'Consumer Discretionary', 'Broadline Retail', 'Seattle, Washington', '0001018724', '1994'),
('NVDA', 'Nvidia', 'Information Technology', 'Semiconductors', 'Santa Clara, California', '0001045810', '1993'),
('GOOGL', 'Alphabet Inc. (Class A)', 'Communication Services', 'Interactive Media & Services', 'Mountain View, California', '0001652044', '1998'),
('GOOG', 'Alphabet Inc. (Class C)', 'Communication Services', 'Interactive Media & Services', 'Mountain View, California', '0001652044', '1998'),
('META', 'Meta Platforms', 'Communication Services', 'Interactive Media & Services', 'Menlo Park, California', '0001326801', '2004'),
('TSLA', 'Tesla Inc.', 'Consumer Discretionary', 'Automobile Manufacturers', 'Austin, Texas', '0001318605', '2003'),
('BRK.B', 'Berkshire Hathaway', 'Financials', 'Multi-Sector Holdings', 'Omaha, Nebraska', '0001067983', '1839'),
('JPM', 'JPMorgan Chase', 'Financials', 'Diversified Banks', 'New York, New York', '0000019617', '2000'),
('V', 'Visa Inc.', 'Financials', 'Transaction & Payment Processing', 'San Francisco, California', '0001403161', '1958'),
('JNJ', 'Johnson & Johnson', 'Health Care', 'Pharmaceuticals', 'New Brunswick, New Jersey', '0000200406', '1886'),
('UNH', 'UnitedHealth Group', 'Health Care', 'Managed Health Care', 'Minnetonka, Minnesota', '0000731766', '1977'),
('WMT', 'Walmart', 'Consumer Staples', 'Consumer Staples Merch', 'Bentonville, Arkansas', '0000104169', '1962'),
('XOM', 'ExxonMobil', 'Energy', 'Integrated Oil & Gas', 'Spring, Texas', '0000034088', '1999'),
('MA', 'Mastercard', 'Financials', 'Transaction & Payment Processing', 'Purchase, New York', '0001141391', '1966'),
('PG', 'Procter & Gamble', 'Consumer Staples', 'Household Products', 'Cincinnati, Ohio', '0000080424', '1837'),
('HD', 'Home Depot', 'Consumer Discretionary', 'Home Improvement Retail', 'Atlanta, Georgia', '0000354950', '1978'),
('CVX', 'Chevron Corporation', 'Energy', 'Integrated Oil & Gas', 'San Ramon, California', '0000093410', '1879'),
('LLY', 'Eli Lilly and Company', 'Health Care', 'Pharmaceuticals', 'Indianapolis, Indiana', '0000059478', '1876'),
('MRK', 'Merck & Co.', 'Health Care', 'Pharmaceuticals', 'Rahway, New Jersey', '0000310158', '1891'),
('ABBV', 'AbbVie', 'Health Care', 'Biotechnology', 'North Chicago, Illinois', '0001551152', '2013'),
('PEP', 'PepsiCo', 'Consumer Staples', 'Soft Drinks & Non-alcoholic', 'Purchase, New York', '0000077476', '1965'),
('KO', 'Coca-Cola Company', 'Consumer Staples', 'Soft Drinks & Non-alcoholic', 'Atlanta, Georgia', '0000021344', '1886'),
('AVGO', 'Broadcom Inc.', 'Information Technology', 'Semiconductors', 'Palo Alto, California', '0001730168', '1961'),
('COST', 'Costco', 'Consumer Staples', 'Consumer Staples Merch', 'Issaquah, Washington', '0000909832', '1983'),
('BAC', 'Bank of America', 'Financials', 'Diversified Banks', 'Charlotte, North Carolina', '0000070858', '1998'),
('TMO', 'Thermo Fisher Scientific', 'Health Care', 'Life Sciences Tools & Services', 'Waltham, Massachusetts', '0000097745', '1956'),
('CSCO', 'Cisco Systems', 'Information Technology', 'Communications Equipment', 'San Jose, California', '0000858877', '1984'),
('MCD', 'McDonalds', 'Consumer Discretionary', 'Restaurants', 'Chicago, Illinois', '0000063908', '1940'),
('ACN', 'Accenture', 'Information Technology', 'IT Consulting & Other Services', 'Dublin, Ireland', '0001467373', '1989'),
('ABT', 'Abbott Laboratories', 'Health Care', 'Health Care Equipment', 'North Chicago, Illinois', '0000001800', '1888'),
('WFC', 'Wells Fargo', 'Financials', 'Diversified Banks', 'San Francisco, California', '0000072971', '1852'),
('ADBE', 'Adobe Inc.', 'Information Technology', 'Application Software', 'San Jose, California', '0000796343', '1982'),
('CRM', 'Salesforce', 'Information Technology', 'Application Software', 'San Francisco, California', '0001108524', '1999'),
('AMD', 'Advanced Micro Devices', 'Information Technology', 'Semiconductors', 'Santa Clara, California', '0000002488', '1969'),
('NFLX', 'Netflix', 'Communication Services', 'Movies & Entertainment', 'Los Gatos, California', '0001065280', '1997'),
('ORCL', 'Oracle Corporation', 'Information Technology', 'Application Software', 'Austin, Texas', '0001341439', '1977'),
('DIS', 'Walt Disney Company', 'Communication Services', 'Movies & Entertainment', 'Burbank, California', '0001744489', '1923'),
('INTC', 'Intel', 'Information Technology', 'Semiconductors', 'Santa Clara, California', '0000050863', '1968'),
('INTU', 'Intuit', 'Information Technology', 'Application Software', 'Mountain View, California', '0000896878', '1983'),
('QCOM', 'Qualcomm', 'Information Technology', 'Semiconductors', 'San Diego, California', '0000804328', '1985'),
('TXN', 'Texas Instruments', 'Information Technology', 'Semiconductors', 'Dallas, Texas', '0000097476', '1951'),
('IBM', 'IBM', 'Information Technology', 'IT Consulting & Other Services', 'Armonk, New York', '0000051143', '1911'),
('GS', 'Goldman Sachs', 'Financials', 'Investment Banking', 'New York, New York', '0000886982', '1869'),
('MS', 'Morgan Stanley', 'Financials', 'Investment Banking', 'New York, New York', '0000895421', '1935'),
('CAT', 'Caterpillar Inc.', 'Industrials', 'Construction Machinery', 'Irving, Texas', '0000018230', '1925'),
('GE', 'GE Aerospace', 'Industrials', 'Aerospace & Defense', 'Evendale, Ohio', '0000040545', '1892'),
('BA', 'Boeing', 'Industrials', 'Aerospace & Defense', 'Arlington, Virginia', '0000012927', '1916'),
('RTX', 'RTX Corporation', 'Industrials', 'Aerospace & Defense', 'Arlington, Virginia', '0000101829', '2020');

-- ============================================
-- SAMPLE DATA: NYSE_SECURITIES (Sample 50)
-- ============================================

INSERT INTO NYSE_SECURITIES (SYMBOL, COMPANY_NAME, ISIN) VALUES
('JPM', 'JPMorgan Chase & Co.', 'US46625H1005'),
('BAC', 'Bank of America Corporation', 'US0605051046'),
('WFC', 'Wells Fargo & Company', 'US9497461015'),
('C', 'Citigroup Inc.', 'US1729674242'),
('GS', 'The Goldman Sachs Group Inc.', 'US38141G1040'),
('MS', 'Morgan Stanley', 'US6174464486'),
('BLK', 'BlackRock Inc.', 'US09247X1019'),
('SCHW', 'The Charles Schwab Corporation', 'US8085131055'),
('AXP', 'American Express Company', 'US0258161092'),
('USB', 'U.S. Bancorp', 'US9029733048'),
('PNC', 'The PNC Financial Services Group Inc.', 'US6934751057'),
('TFC', 'Truist Financial Corporation', 'US89832Q1094'),
('COF', 'Capital One Financial Corporation', 'US14040H1059'),
('BK', 'The Bank of New York Mellon Corporation', 'US0640581007'),
('STT', 'State Street Corporation', 'US8574771031'),
('XOM', 'Exxon Mobil Corporation', 'US30231G1022'),
('CVX', 'Chevron Corporation', 'US1667641005'),
('COP', 'ConocoPhillips', 'US20825C1045'),
('SLB', 'Schlumberger Limited', 'AN8068571086'),
('EOG', 'EOG Resources Inc.', 'US26875P1012'),
('PXD', 'Pioneer Natural Resources Company', 'US7237871071'),
('MPC', 'Marathon Petroleum Corporation', 'US56585A1025'),
('VLO', 'Valero Energy Corporation', 'US91913Y1001'),
('PSX', 'Phillips 66', 'US7185461040'),
('OXY', 'Occidental Petroleum Corporation', 'US6745991058'),
('JNJ', 'Johnson & Johnson', 'US4781601046'),
('UNH', 'UnitedHealth Group Incorporated', 'US91324P1021'),
('PFE', 'Pfizer Inc.', 'US7170811035'),
('MRK', 'Merck & Co. Inc.', 'US58933Y1055'),
('ABT', 'Abbott Laboratories', 'US0028241000'),
('TMO', 'Thermo Fisher Scientific Inc.', 'US8835561023'),
('DHR', 'Danaher Corporation', 'US2358511028'),
('BMY', 'Bristol-Myers Squibb Company', 'US1101221083'),
('AMGN', 'Amgen Inc.', 'US0311621009'),
('GILD', 'Gilead Sciences Inc.', 'US3755581036'),
('WMT', 'Walmart Inc.', 'US9311421039'),
('PG', 'The Procter & Gamble Company', 'US7427181091'),
('KO', 'The Coca-Cola Company', 'US1912161007'),
('PEP', 'PepsiCo Inc.', 'US7134481081'),
('COST', 'Costco Wholesale Corporation', 'US22160K1051'),
('PM', 'Philip Morris International Inc.', 'US7181721090'),
('MO', 'Altria Group Inc.', 'US02209S1033'),
('CL', 'Colgate-Palmolive Company', 'US1941621039'),
('MDLZ', 'Mondelez International Inc.', 'US6092071058'),
('KHC', 'The Kraft Heinz Company', 'US5007541064'),
('HD', 'The Home Depot Inc.', 'US4370761029'),
('LOW', 'Lowes Companies Inc.', 'US5486611073'),
('TGT', 'Target Corporation', 'US87612E1064'),
('TJX', 'The TJX Companies Inc.', 'US8725401090'),
('NKE', 'NIKE Inc.', 'US6541061031');

-- ============================================
-- SAMPLE DATA: CORPORATE_BONDS
-- ============================================

USE SCHEMA FIXED_INCOME;

INSERT INTO CORPORATE_BONDS (BOND_ID, CUSIP, ISIN, FIGI, TICKER, ISSUER_NAME, ISSUE_DATE, MATURITY_DATE, COUPON_RATE, COUPON_FREQUENCY, CURRENT_YIELD, CREDIT_RATING, PAR_VALUE, CURRENCY, BOND_TYPE, CALLABLE, SECTOR) VALUES
(1, '037833DU6', 'US037833DU68', 'BBG00N3LXWZ7', 'AAPL', 'Apple Inc', '2020-05-11', '2050-05-11', 2.650, 'Semi-Annual', 2.750, 'AA+', 2500000000.00, 'USD', 'Senior Unsecured', TRUE, 'Technology'),
(2, '037833EB7', 'US037833EB76', 'BBG00PDQNZ58', 'AAPL', 'Apple Inc', '2021-08-20', '2031-08-20', 1.400, 'Semi-Annual', 1.520, 'AA+', 1500000000.00, 'USD', 'Senior Unsecured', FALSE, 'Technology'),
(3, '594918CC4', 'US594918CC45', 'BBG00QXZM0P2', 'MSFT', 'Microsoft Corporation', '2020-06-01', '2050-06-01', 2.525, 'Semi-Annual', 2.680, 'AAA', 3000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Technology'),
(4, '594918BZ4', 'US594918BZ47', 'BBG00NB4RSK4', 'MSFT', 'Microsoft Corporation', '2020-02-12', '2060-02-12', 2.675, 'Semi-Annual', 2.820, 'AAA', 2000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Technology'),
(5, '023135BW7', 'US023135BW78', 'BBG00LVPWPJ5', 'AMZN', 'Amazon.com Inc', '2021-05-12', '2051-05-12', 3.250, 'Semi-Annual', 3.380, 'AA', 2750000000.00, 'USD', 'Senior Unsecured', TRUE, 'Consumer Discretionary'),
(6, '46625HRX5', 'US46625HRX51', 'BBG00QYZJ6R8', 'JPM', 'JPMorgan Chase & Co', '2021-04-22', '2032-04-22', 2.522, 'Semi-Annual', 2.650, 'A-', 3500000000.00, 'USD', 'Senior Unsecured', FALSE, 'Financials'),
(7, '46625HRW7', 'US46625HRW78', 'BBG00QYZJ5T6', 'JPM', 'JPMorgan Chase & Co', '2021-04-22', '2051-04-22', 3.328, 'Semi-Annual', 3.480, 'A-', 2500000000.00, 'USD', 'Senior Unsecured', TRUE, 'Financials'),
(8, '060505FL8', 'US060505FL88', 'BBG00RDZPKM1', 'BAC', 'Bank of America Corporation', '2021-07-21', '2031-07-21', 1.898, 'Semi-Annual', 2.050, 'A-', 2000000000.00, 'USD', 'Senior Unsecured', FALSE, 'Financials'),
(9, '172967MR8', 'US172967MR81', 'BBG00QHDNZ87', 'C', 'Citigroup Inc', '2021-01-29', '2031-01-29', 2.014, 'Semi-Annual', 2.180, 'BBB+', 2250000000.00, 'USD', 'Senior Unsecured', FALSE, 'Financials'),
(10, '38141GYC5', 'US38141GYC58', 'BBG00RDNHT81', 'GS', 'Goldman Sachs Group Inc', '2021-07-21', '2032-07-21', 2.615, 'Semi-Annual', 2.780, 'BBB+', 2750000000.00, 'USD', 'Senior Unsecured', FALSE, 'Financials'),
(11, '30231GAV5', 'US30231GAV59', 'BBG00MCMVD17', 'XOM', 'Exxon Mobil Corporation', '2020-03-06', '2050-03-06', 3.452, 'Semi-Annual', 3.580, 'AA-', 1500000000.00, 'USD', 'Senior Unsecured', TRUE, 'Energy'),
(12, '166764BV3', 'US166764BV31', 'BBG00MCMVQ66', 'CVX', 'Chevron Corporation', '2020-05-11', '2050-05-11', 3.078, 'Semi-Annual', 3.220, 'AA-', 1750000000.00, 'USD', 'Senior Unsecured', TRUE, 'Energy'),
(13, '717081ER9', 'US717081ER98', 'BBG00QNWPL88', 'PFE', 'Pfizer Inc', '2021-03-11', '2041-03-11', 2.550, 'Semi-Annual', 2.680, 'A', 1250000000.00, 'USD', 'Senior Unsecured', TRUE, 'Healthcare'),
(14, '478160CD4', 'US478160CD42', 'BBG00NZWXV17', 'JNJ', 'Johnson & Johnson', '2020-09-01', '2050-09-01', 2.100, 'Semi-Annual', 2.250, 'AAA', 1000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Healthcare'),
(15, '91324PDN1', 'US91324PDN14', 'BBG00QBBTJQ5', 'UNH', 'UnitedHealth Group Incorporated', '2021-02-15', '2051-02-15', 3.250, 'Semi-Annual', 3.380, 'A+', 1500000000.00, 'USD', 'Senior Unsecured', TRUE, 'Healthcare'),
(16, '931142EM3', 'US931142EM31', 'BBG00QHDN2P7', 'WMT', 'Walmart Inc', '2021-04-15', '2051-04-15', 2.850, 'Semi-Annual', 2.980, 'AA', 1250000000.00, 'USD', 'Senior Unsecured', TRUE, 'Consumer Staples'),
(17, '742718FA3', 'US742718FA31', 'BBG00QZDRZM8', 'PG', 'Procter & Gamble Company', '2021-03-25', '2051-03-25', 2.800, 'Semi-Annual', 2.920, 'AA-', 1000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Consumer Staples'),
(18, '191216DA3', 'US191216DA30', 'BBG00QMCXN17', 'KO', 'Coca-Cola Company', '2021-03-08', '2051-03-08', 2.875, 'Semi-Annual', 3.010, 'A+', 1500000000.00, 'USD', 'Senior Unsecured', TRUE, 'Consumer Staples'),
(19, '92343VGH9', 'US92343VGH96', 'BBG00LCWZ6J5', 'VZ', 'Verizon Communications Inc', '2020-03-20', '2050-03-20', 4.000, 'Semi-Annual', 4.150, 'BBB+', 2000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Communication Services'),
(20, '00206RKH1', 'US00206RKH10', 'BBG00MCMVR88', 'T', 'AT&T Inc', '2020-06-01', '2060-06-01', 3.650, 'Semi-Annual', 3.820, 'BBB', 1750000000.00, 'USD', 'Senior Unsecured', TRUE, 'Communication Services'),
(21, '097023DG3', 'US097023DG38', 'BBG00QZDQT17', 'BA', 'Boeing Company', '2020-05-01', '2050-05-01', 5.805, 'Semi-Annual', 5.950, 'BBB-', 3500000000.00, 'USD', 'Senior Unsecured', FALSE, 'Industrials'),
(22, '345370CX6', 'US345370CX63', 'BBG00MCMVKZ7', 'F', 'Ford Motor Company', '2020-04-22', '2030-04-22', 9.000, 'Semi-Annual', 5.250, 'BB+', 2000000000.00, 'USD', 'Senior Unsecured', FALSE, 'Consumer Discretionary'),
(23, '88160RAP1', 'US88160RAP10', 'BBG00RDZP6N8', 'TSLA', 'Tesla Inc', '2021-08-13', '2031-08-13', 2.375, 'Semi-Annual', 2.520, 'BB', 1500000000.00, 'USD', 'Senior Unsecured', FALSE, 'Consumer Discretionary'),
(24, '842587DB6', 'US842587DB66', 'BBG00QMNP9K5', 'SO', 'Southern Company', '2021-03-15', '2051-03-15', 3.700, 'Semi-Annual', 3.850, 'BBB+', 1000000000.00, 'USD', 'Senior Unsecured', TRUE, 'Utilities'),
(25, '26441CAZ1', 'US26441CAZ14', 'BBG00QMNP7M6', 'DUK', 'Duke Energy Corporation', '2021-03-15', '2052-03-15', 3.300, 'Semi-Annual', 3.450, 'BBB+', 1250000000.00, 'USD', 'Senior Unsecured', TRUE, 'Utilities');

-- ============================================
-- SAMPLE DATA: SECURITY_MASTER_REFERENCE
-- ============================================

USE SCHEMA GOLDEN_RECORD;

INSERT INTO SECURITY_MASTER_REFERENCE 
(GLOBAL_SECURITY_ID, ISSUER, ASSET_CLASS, PRIMARY_TICKER, PRIMARY_EXCHANGE, ISIN, CUSIP, SEDOL, CURRENCY, STATUS, GOLDEN_SOURCE, LAST_VALIDATED, LINEAGE_ID, CREATED_BY, CREATED_AT, LAST_MODIFIED_BY)
VALUES
('GSID-000001', 'Apple Inc.', 'Equity', 'AAPL', 'NASDAQ', 'US0378331005', '037833100', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000001-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000002', 'Microsoft Corporation', 'Equity', 'MSFT', 'NASDAQ', 'US5949181045', '594918104', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000002-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000003', 'Amazon.com Inc.', 'Equity', 'AMZN', 'NASDAQ', 'US0231351067', '023135106', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000003-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000004', 'Alphabet Inc. Class A', 'Equity', 'GOOGL', 'NASDAQ', 'US02079K3059', '02079K305', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000004-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000005', 'NVIDIA Corporation', 'Equity', 'NVDA', 'NASDAQ', 'US67066G1040', '67066G104', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000005-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000006', 'Tesla Inc.', 'Equity', 'TSLA', 'NASDAQ', 'US88160R1014', '88160R101', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000006-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000007', 'Meta Platforms Inc.', 'Equity', 'META', 'NASDAQ', 'US30303M1027', '30303M102', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000007-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000008', 'Berkshire Hathaway Inc.', 'Equity', 'BRK.B', 'NYSE', 'US0846707026', '084670702', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000008-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000009', 'JPMorgan Chase & Co.', 'Equity', 'JPM', 'NYSE', 'US46625H1005', '46625H100', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000009-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000010', 'Johnson & Johnson', 'Equity', 'JNJ', 'NYSE', 'US4781601046', '478160104', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000010-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000011', 'Visa Inc.', 'Equity', 'V', 'NYSE', 'US92826C8394', '92826C839', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000011-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000012', 'UnitedHealth Group Inc.', 'Equity', 'UNH', 'NYSE', 'US91324P1021', '91324P102', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000012-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000013', 'HSBC Holdings plc', 'Equity', 'HSBA', 'LSE', 'GB0005405286', NULL, '0540528', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000013-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000014', 'BP plc', 'Equity', 'BP.', 'LSE', 'GB0007980591', NULL, '0798059', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000014-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000015', 'Shell plc', 'Equity', 'SHEL', 'LSE', 'GB00BP6MXD84', NULL, 'BP6MXD8', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000015-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000016', 'AstraZeneca plc', 'Equity', 'AZN', 'LSE', 'GB0009895292', NULL, '0989529', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000016-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000017', 'Unilever plc', 'Equity', 'ULVR', 'LSE', 'GB00B10RZP78', NULL, 'B10RZP7', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000017-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000018', 'Rio Tinto plc', 'Equity', 'RIO', 'LSE', 'GB0007188757', NULL, '0718875', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000018-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000019', 'Glencore plc', 'Equity', 'GLEN', 'LSE', 'JE00B4T3BW64', NULL, 'B4T3BW6', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000019-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000020', 'Diageo plc', 'Equity', 'DGE', 'LSE', 'GB0002374006', NULL, '0237400', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000020-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000021', 'US Treasury Bond 4.5% 2034', 'Fixed Income', 'T 4.5 02/15/34', 'OTC', 'US912810TM53', '912810TM5', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000021-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000022', 'US Treasury Bond 4.0% 2029', 'Fixed Income', 'T 4 11/15/29', 'OTC', 'US91282CJV63', '91282CJV6', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000022-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000023', 'Apple Inc. 3.85% 2043', 'Fixed Income', 'AAPL 3.85 05/04/43', 'OTC', 'US037833DU68', '037833DU6', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000023-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000024', 'Microsoft Corp 2.525% 2050', 'Fixed Income', 'MSFT 2.525 06/01/50', 'OTC', 'US594918CC45', '594918CC4', NULL, 'USD', 'ACTIVE', 'Bloomberg Terminal', CURRENT_TIMESTAMP(), 'LIN-GSID-000024-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM'),
('GSID-000025', 'UK Gilt 4.25% 2034', 'Fixed Income', 'UKT 4.25 12/07/34', 'LSE', 'GB00BMGR2916', NULL, 'BMGR291', 'GBP', 'ACTIVE', 'Refinitiv Eikon', CURRENT_TIMESTAMP(), 'LIN-GSID-000025-20260115120000', 'SYSTEM', '2026-01-15 12:00:00', 'SYSTEM');

-- ============================================
-- SAMPLE DATA: SECURITY_MASTER_HISTORY
-- ============================================

INSERT INTO SECURITY_MASTER_HISTORY 
(GLOBAL_SECURITY_ID, ACTION, ISSUER_BEFORE, ISSUER_AFTER, ASSET_CLASS_BEFORE, ASSET_CLASS_AFTER, PRIMARY_TICKER_BEFORE, PRIMARY_TICKER_AFTER, PRIMARY_EXCHANGE_BEFORE, PRIMARY_EXCHANGE_AFTER, ISIN_BEFORE, ISIN_AFTER, CUSIP_BEFORE, CUSIP_AFTER, SEDOL_BEFORE, SEDOL_AFTER, CURRENCY_BEFORE, CURRENCY_AFTER, STATUS_BEFORE, STATUS_AFTER, EDIT_REASON, CHANGED_BY, CHANGED_AT, SOURCE_SYSTEM, LINEAGE_ID, LINEAGE_PARENT_ID, LINEAGE_PATH)
VALUES
('GSID-000001', 'INSERT', NULL, 'Apple Inc.', NULL, 'Equity', NULL, 'AAPL', NULL, 'NASDAQ', NULL, 'US0378331005', NULL, '037833100', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000001-20260115120000', NULL, 'LIN-GSID-000001-20260115120000'),
('GSID-000002', 'INSERT', NULL, 'Microsoft Corporation', NULL, 'Equity', NULL, 'MSFT', NULL, 'NASDAQ', NULL, 'US5949181045', NULL, '594918104', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000002-20260115120000', NULL, 'LIN-GSID-000002-20260115120000'),
('GSID-000003', 'INSERT', NULL, 'Amazon.com Inc.', NULL, 'Equity', NULL, 'AMZN', NULL, 'NASDAQ', NULL, 'US0231351067', NULL, '023135106', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000003-20260115120000', NULL, 'LIN-GSID-000003-20260115120000'),
('GSID-000004', 'INSERT', NULL, 'Alphabet Inc. Class A', NULL, 'Equity', NULL, 'GOOGL', NULL, 'NASDAQ', NULL, 'US02079K3059', NULL, '02079K305', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000004-20260115120000', NULL, 'LIN-GSID-000004-20260115120000'),
('GSID-000005', 'INSERT', NULL, 'NVIDIA Corporation', NULL, 'Equity', NULL, 'NVDA', NULL, 'NASDAQ', NULL, 'US67066G1040', NULL, '67066G104', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000005-20260115120000', NULL, 'LIN-GSID-000005-20260115120000'),
('GSID-000006', 'INSERT', NULL, 'Tesla Inc.', NULL, 'Equity', NULL, 'TSLA', NULL, 'NASDAQ', NULL, 'US88160R1014', NULL, '88160R101', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000006-20260115120000', NULL, 'LIN-GSID-000006-20260115120000'),
('GSID-000007', 'INSERT', NULL, 'Meta Platforms Inc.', NULL, 'Equity', NULL, 'META', NULL, 'NASDAQ', NULL, 'US30303M1027', NULL, '30303M102', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'Initial security creation', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000007-20260115120000', NULL, 'LIN-GSID-000007-20260115120000'),
('GSID-000007', 'UPDATE', 'Facebook Inc.', 'Meta Platforms Inc.', 'Equity', 'Equity', 'FB', 'META', 'NASDAQ', 'NASDAQ', 'US30303M1027', 'US30303M1027', '30303M102', '30303M102', NULL, NULL, 'USD', 'USD', 'ACTIVE', 'ACTIVE', 'Corporate rebranding from Facebook to Meta Platforms', 'ADMIN', '2026-01-16 09:30:00', 'Security Master EDM', 'LIN-GSID-000007-20260116093000', 'LIN-GSID-000007-20260115120000', 'LIN-GSID-000007-20260115120000 -> LIN-GSID-000007-20260116093000'),
('GSID-000013', 'INSERT', NULL, 'HSBC Holdings plc', NULL, 'Equity', NULL, 'HSBA', NULL, 'LSE', NULL, 'GB0005405286', NULL, NULL, NULL, '0540528', NULL, 'GBP', NULL, 'ACTIVE', 'Initial security creation - LSEG feed', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000013-20260115120000', NULL, 'LIN-GSID-000013-20260115120000'),
('GSID-000021', 'INSERT', NULL, 'US Treasury Bond 4.5% 2034', NULL, 'Fixed Income', NULL, 'T 4.5 02/15/34', NULL, 'OTC', NULL, 'US912810TM53', NULL, '912810TM5', NULL, NULL, NULL, 'USD', NULL, 'ACTIVE', 'New Treasury issuance', 'SYSTEM', '2026-01-15 12:00:00', 'Security Master EDM', 'LIN-GSID-000021-20260115120000', NULL, 'LIN-GSID-000021-20260115120000');

-- ============================================
-- VERIFICATION
-- ============================================

SELECT 'Setup Complete!' AS STATUS;

SELECT 'SECURITIES.SP500' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM COLM_DB.SECURITIES.SP500
UNION ALL SELECT 'SECURITIES.NYSE_SECURITIES', COUNT(*) FROM COLM_DB.SECURITIES.NYSE_SECURITIES
UNION ALL SELECT 'FIXED_INCOME.CORPORATE_BONDS', COUNT(*) FROM COLM_DB.FIXED_INCOME.CORPORATE_BONDS
UNION ALL SELECT 'GOLDEN_RECORD.SECURITY_MASTER_REFERENCE', COUNT(*) FROM COLM_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
UNION ALL SELECT 'GOLDEN_RECORD.SECURITY_MASTER_HISTORY', COUNT(*) FROM COLM_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
ORDER BY TABLE_NAME;

SHOW SEQUENCES IN SCHEMA COLM_DB.GOLDEN_RECORD;
SHOW FUNCTIONS IN SCHEMA COLM_DB.GOLDEN_RECORD;
SHOW STAGES IN SCHEMA COLM_DB.SECURITIES;
