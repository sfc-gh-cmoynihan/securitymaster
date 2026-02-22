import csv
import json
import time
import requests
import os
import snowflake.connector

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE = 10

def load_tickers(csv_path):
    tickers = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row.get('ACT Symbol', '').strip()
            name = row.get('Company Name', '').strip()
            if symbol and '$' not in symbol and '.' not in symbol:
                tickers.append({'symbol': symbol, 'name': name})
    return tickers

def lookup_isins_batch(tickers):
    jobs = [{"idType": "TICKER", "idValue": t['symbol'], "exchCode": "US"} for t in tickers]
    
    headers = {"Content-Type": "application/json"}
    response = requests.post(OPENFIGI_URL, headers=headers, json=jobs)
    
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print("Rate limited, waiting 60 seconds...")
        time.sleep(60)
        return lookup_isins_batch(tickers)
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return [{"error": "API error"}] * len(tickers)

def main():
    print("Loading tickers from CSV...")
    tickers = load_tickers('nyse_listed.csv')
    print(f"Loaded {len(tickers)} tickers (excluding preferred shares and special classes)")
    
    results = []
    
    print("Looking up ISINs via OpenFIGI API...")
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(tickers) + BATCH_SIZE - 1)//BATCH_SIZE}...")
        
        figi_results = lookup_isins_batch(batch)
        
        for j, (ticker, figi_result) in enumerate(zip(batch, figi_results)):
            isin = None
            if isinstance(figi_result, dict) and 'data' in figi_result:
                for item in figi_result['data']:
                    if item.get('shareClassFIGI'):
                        isin = item.get('shareClassFIGI')[:12] if item.get('shareClassFIGI') else None
                    if 'compositeFIGI' in item:
                        for d in figi_result.get('data', []):
                            if d.get('securityType') == 'Common Stock':
                                isin = d.get('shareClassFIGI', '')[:12] if d.get('shareClassFIGI') else None
            
            results.append({
                'symbol': ticker['symbol'],
                'name': ticker['name'],
                'isin': isin
            })
        
        time.sleep(0.5)
    
    with open('nyse_with_isins.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nProcessed {len(results)} securities")
    print(f"Found ISINs for {sum(1 for r in results if r['isin'])} securities")
    
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "colms_uswest")
    
    cursor = conn.cursor()
    
    cursor.execute("CREATE DATABASE IF NOT EXISTS SECURITIES_MASTER")
    cursor.execute("USE DATABASE SECURITIES_MASTER")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS EQUITY")
    cursor.execute("USE SCHEMA EQUITY")
    
    cursor.execute("""
        CREATE OR REPLACE TABLE NYSE_SECURITIES (
            SYMBOL VARCHAR(20) NOT NULL,
            COMPANY_NAME VARCHAR(500),
            ISIN VARCHAR(12),
            EXCHANGE VARCHAR(10) DEFAULT 'NYSE',
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    print("Inserting data into Snowflake...")
    insert_sql = "INSERT INTO NYSE_SECURITIES (SYMBOL, COMPANY_NAME, ISIN) VALUES (%s, %s, %s)"
    
    for r in results:
        cursor.execute(insert_sql, (r['symbol'], r['name'], r['isin']))
    
    cursor.execute("SELECT COUNT(*) FROM NYSE_SECURITIES")
    count = cursor.fetchone()[0]
    print(f"\nInserted {count} records into SECURITIES_MASTER.EQUITY.NYSE_SECURITIES")
    
    cursor.execute("SELECT * FROM NYSE_SECURITIES LIMIT 10")
    print("\nSample data:")
    for row in cursor.fetchall():
        print(row)
    
    cursor.close()
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
