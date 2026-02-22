/*-----------------------------------------------------------------------
Interactice Analytics to Make Snowflake Intelligence fast on large tables
1. Create an interactice warehouse
2. Create an interactive Table
3. Associate the Interactive Table with the interactive Warehouse
4. Run some queries to compare to standard Warehouse
5. Create a semanatic view
6. Show the Agent 
7. Demo Snowflake Intelligence
-----------------------------------------------------------------------*/
USE DATABASE INTERACTIVE_DB;
USE SCHEMA PUBLIC;

--1. Create an interactice warehouse
CREATE or REPLACE INTERACTIVE WAREHOUSE INTERACTIVE_WH_SI WAREHOUSE_SIZE = 'SMALL';

--2. Create an interactive Table
USE WAREHOUSE LARGE_WH;

CREATE OR REPLACE INTERACTIVE TABLE INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT
  CLUSTER BY (TICKER, DATE)
  COMMENT = 'Interactive table for real-time Agentic AI'
AS
  select * from SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.STOCK_PRICE_TIMESERIES;

-- CREATE FDN TABLE 
CREATE OR REPLACE TABLE INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES
  CLUSTER BY (TICKER, DATE)
  COMMENT = 'Interactive table for real-time Agentic AI'
AS
  select * from SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.STOCK_PRICE_TIMESERIES;  

DESCRIBE TABLE STOCK_PRICE_TIMESERIES_IT;
  
--3. Associate the Interactive Table with the interactive Warehouse
ALTER WAREHOUSE INTERACTIVE_WH_SI 
ADD TABLES (INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT);

--Resume warehouse
ALTER WAREHOUSE INTERACTIVE_WH_SI RESUME IF SUSPENDED;

--4. Run some queries to compare to standard Warehouse
USE WAREHOUSE INTERACTIVE_WH_SI;
select count(*) from STOCK_PRICE_TIMESERIES_IT; --156 MILLION ROWS

SELECT 
    TICKER,
    ASSET_CLASS,
    MIN(DATE) AS EARLIEST_DATE,
    PRIMARY_EXCHANGE_CODE,
    MAX(DATE) AS LATEST_DATE,
    COUNT(*) AS RECORD_COUNT,
    COUNT(DISTINCT VARIABLE) AS VARIABLES
FROM INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT
WHERE ASSET_CLASS = 'Equity'
GROUP BY TICKER, ASSET_CLASS, PRIMARY_EXCHANGE_CODE
ORDER BY TICKER; -- Less than 1 second response time

-- RUN IT ON A STANDARD WAREHOUSE MEDIUM
CREATE or REPLACE WAREHOUSE SMALL_WH WAREHOUSE_SIZE = 'SMALL';
ALTER WAREHOUSE SMALL_WH RESUME IF SUSPENDED;

SELECT 
    TICKER,
    MIN(DATE) AS EARLIEST_DATE,
    PRIMARY_EXCHANGE_CODE,
    MAX(DATE) AS LATEST_DATE,
    COUNT(DISTINCT VARIABLE) AS VARIABLES,
    COUNT(*) AS RECORD_COUNT
FROM INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT
WHERE ASSET_CLASS = 'Equity'
GROUP BY TICKER, ASSET_CLASS, PRIMARY_EXCHANGE_CODE
ORDER BY TICKER; -- 3 seconds 6 x slower 

-- Greater than 3 seconds is annoying in a BI use case you want sub second

--5. Create a semanatic view on Interactive table 
create or replace semantic view INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT_SV
	tables (
		INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_IT
	)
	facts (
		STOCK_PRICE_TIMESERIES_IT.VALUE as VALUE comment='Value reported for the variable.'
	)
	dimensions (
		STOCK_PRICE_TIMESERIES_IT.ASSET_CLASS as ASSET_CLASS comment='Type of security.',
		STOCK_PRICE_TIMESERIES_IT.PRIMARY_EXCHANGE_CODE as PRIMARY_EXCHANGE_CODE comment='The exchange code for the primary trading venue of a security.',
		STOCK_PRICE_TIMESERIES_IT.PRIMARY_EXCHANGE_NAME as PRIMARY_EXCHANGE_NAME comment='The exchange name for the primary trading venue of a security.',
		STOCK_PRICE_TIMESERIES_IT.TICKER as TICKER comment='Alphanumeric code that represents a specific publicly traded security on the NASDAQ exchange.',
		STOCK_PRICE_TIMESERIES_IT.VARIABLE as VARIABLE comment='Unique identifier for a variable.',
		STOCK_PRICE_TIMESERIES_IT.VARIABLE_NAME as VARIABLE_NAME comment='Human-readable unique name for the variable.',
		STOCK_PRICE_TIMESERIES_IT.DATE as DATE comment='Date associated with the value.',
		STOCK_PRICE_TIMESERIES_IT.EVENT_TIMESTAMP_UTC as EVENT_TIMESTAMP_UTC comment='Timestamp when the event occurred in UTC.'
	)
	comment='SECURITIES'
	with extension (CA='{"tables":[{"name":"STOCK_PRICE_TIMESERIES_IT","dimensions":[{"name":"ASSET_CLASS","sample_values":["Common Shares","Closed-End Funds","Equity"]},{"name":"PRIMARY_EXCHANGE_CODE","sample_values":["NYS","PSE","NAS"]},{"name":"PRIMARY_EXCHANGE_NAME","sample_values":["NEW YORK STOCK EXCHANGE","NASDAQ CAPITAL MARKET","NYSE ARCA"]},{"name":"TICKER","sample_values":["USB","CWT","MIT"]},{"name":"VARIABLE","sample_values":["all-day_high_adjusted","pre-market_open","post-market_close"]},{"name":"VARIABLE_NAME","sample_values":["Nasdaq Volume","All-Day High","Post-Market Close"]}],"facts":[{"name":"VALUE","sample_values":["50.8","27.21","28.99"]}],"filters":[{"name":"major_tech_stocks","description":"Filters for major technology stocks including Amazon (AMZN), Microsoft (MSFT), and Snowflake (SNOW). Use when questions ask about ''tech stocks'', ''major technology companies'', ''FAANG stocks'', ''cloud companies'', ''Amazon Microsoft Snowflake'', or specific analysis of these three securities. Helps analyze performance and trends among leading technology sector stocks.","expr":"ticker IN (''AMZN'', ''MSFT'', ''SNOW'')"}],"time_dimensions":[{"name":"DATE","sample_values":["2022-10-12","2023-04-25","2019-07-17"]},{"name":"EVENT_TIMESTAMP_UTC","sample_values":["2024-07-16T13:30:00.000+0000","2021-08-06T20:00:00.000+0000","2022-10-24T13:30:00.000+0000"]}]}]}');

-- Create a Semantic view on FDN Table
create or replace semantic view INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES_SV
	tables (
		INTERACTIVE_DB.PUBLIC.STOCK_PRICE_TIMESERIES
	)
	facts (
		STOCK_PRICE_TIMESERIES.VALUE as VALUE comment='Value reported for the variable.'
	)
	dimensions (
		STOCK_PRICE_TIMESERIES.ASSET_CLASS as ASSET_CLASS comment='Type of security.',
		STOCK_PRICE_TIMESERIES.PRIMARY_EXCHANGE_CODE as PRIMARY_EXCHANGE_CODE comment='The exchange code for the primary trading venue of a security.',
		STOCK_PRICE_TIMESERIES.PRIMARY_EXCHANGE_NAME as PRIMARY_EXCHANGE_NAME comment='The exchange name for the primary trading venue of a security.',
		STOCK_PRICE_TIMESERIES.TICKER as TICKER comment='Alphanumeric code that represents a specific publicly traded security on the NASDAQ exchange.',
		STOCK_PRICE_TIMESERIES.VARIABLE as VARIABLE comment='Unique identifier for a variable.',
		STOCK_PRICE_TIMESERIES.VARIABLE_NAME as VARIABLE_NAME comment='Human-readable unique name for the variable.',
		STOCK_PRICE_TIMESERIES.DATE as DATE comment='Date associated with the value.',
		STOCK_PRICE_TIMESERIES.EVENT_TIMESTAMP_UTC as EVENT_TIMESTAMP_UTC comment='Timestamp when the event occurred in UTC.'
	)
	comment='SECURITIES'
	with extension (CA='{"tables":[{"name":"STOCK_PRICE_TIMESERIES_IT","dimensions":[{"name":"ASSET_CLASS","sample_values":["Common Shares","Closed-End Funds","Equity"]},{"name":"PRIMARY_EXCHANGE_CODE","sample_values":["NYS","PSE","NAS"]},{"name":"PRIMARY_EXCHANGE_NAME","sample_values":["NEW YORK STOCK EXCHANGE","NASDAQ CAPITAL MARKET","NYSE ARCA"]},{"name":"TICKER","sample_values":["USB","CWT","MIT"]},{"name":"VARIABLE","sample_values":["all-day_high_adjusted","pre-market_open","post-market_close"]},{"name":"VARIABLE_NAME","sample_values":["Nasdaq Volume","All-Day High","Post-Market Close"]}],"facts":[{"name":"VALUE","sample_values":["50.8","27.21","28.99"]}],"filters":[{"name":"major_tech_stocks","description":"Filters for major technology stocks including Amazon (AMZN), Microsoft (MSFT), and Snowflake (SNOW). Use when questions ask about ''tech stocks'', ''major technology companies'', ''FAANG stocks'', ''cloud companies'', ''Amazon Microsoft Snowflake'', or specific analysis of these three securities. Helps analyze performance and trends among leading technology sector stocks.","expr":"ticker IN (''AMZN'', ''MSFT'', ''SNOW'')"}],"time_dimensions":[{"name":"DATE","sample_values":["2022-10-12","2023-04-25","2019-07-17"]},{"name":"EVENT_TIMESTAMP_UTC","sample_values":["2024-07-16T13:30:00.000+0000","2021-08-06T20:00:00.000+0000","2022-10-24T13:30:00.000+0000"]}]}]}');

--SHOW AGENT
--SNOW SNOWFLAKE INTELLGIENCE 
