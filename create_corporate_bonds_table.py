import os
import random
from datetime import datetime, timedelta
import snowflake.connector

TOP_CORPORATE_ISSUERS = [
    ("AAPL", "Apple Inc", "037833"),
    ("MSFT", "Microsoft Corporation", "594918"),
    ("AMZN", "Amazon.com Inc", "023135"),
    ("GOOG", "Alphabet Inc", "02079K"),
    ("META", "Meta Platforms Inc", "30303M"),
    ("JPM", "JPMorgan Chase & Co", "46625H"),
    ("BAC", "Bank of America Corporation", "060505"),
    ("WFC", "Wells Fargo & Company", "949746"),
    ("C", "Citigroup Inc", "172967"),
    ("GS", "Goldman Sachs Group Inc", "38141G"),
    ("MS", "Morgan Stanley", "617446"),
    ("V", "Visa Inc", "92826C"),
    ("MA", "Mastercard Incorporated", "57636Q"),
    ("XOM", "Exxon Mobil Corporation", "30231G"),
    ("CVX", "Chevron Corporation", "166764"),
    ("PFE", "Pfizer Inc", "717081"),
    ("JNJ", "Johnson & Johnson", "478160"),
    ("UNH", "UnitedHealth Group Incorporated", "91324P"),
    ("HD", "Home Depot Inc", "437076"),
    ("WMT", "Walmart Inc", "931142"),
    ("PG", "Procter & Gamble Company", "742718"),
    ("KO", "Coca-Cola Company", "191216"),
    ("PEP", "PepsiCo Inc", "713448"),
    ("MRK", "Merck & Co Inc", "58933Y"),
    ("ABBV", "AbbVie Inc", "00287Y"),
    ("TMO", "Thermo Fisher Scientific Inc", "883556"),
    ("COST", "Costco Wholesale Corporation", "22160K"),
    ("ABT", "Abbott Laboratories", "002824"),
    ("DIS", "Walt Disney Company", "254687"),
    ("VZ", "Verizon Communications Inc", "92343V"),
    ("T", "AT&T Inc", "00206R"),
    ("CMCSA", "Comcast Corporation", "20030N"),
    ("INTC", "Intel Corporation", "458140"),
    ("IBM", "International Business Machines", "459200"),
    ("ORCL", "Oracle Corporation", "68389X"),
    ("CRM", "Salesforce Inc", "79466L"),
    ("CSCO", "Cisco Systems Inc", "17275R"),
    ("ACN", "Accenture plc", "G1151C"),
    ("TXN", "Texas Instruments Incorporated", "882508"),
    ("QCOM", "QUALCOMM Incorporated", "747525"),
    ("AMD", "Advanced Micro Devices Inc", "007903"),
    ("NVDA", "NVIDIA Corporation", "67066G"),
    ("CAT", "Caterpillar Inc", "149123"),
    ("DE", "Deere & Company", "244199"),
    ("BA", "Boeing Company", "097023"),
    ("GE", "General Electric Company", "369604"),
    ("HON", "Honeywell International Inc", "438516"),
    ("MMM", "3M Company", "88579Y"),
    ("UPS", "United Parcel Service Inc", "911312"),
    ("LMT", "Lockheed Martin Corporation", "539830"),
    ("RTX", "RTX Corporation", "75513E"),
    ("F", "Ford Motor Company", "345370"),
    ("GM", "General Motors Company", "37045V"),
    ("TM", "Toyota Motor Corporation", "892331"),
    ("TSLA", "Tesla Inc", "88160R"),
    ("NKE", "Nike Inc", "654106"),
    ("MCD", "McDonald's Corporation", "580135"),
    ("SBUX", "Starbucks Corporation", "855244"),
    ("LOW", "Lowe's Companies Inc", "548661"),
    ("TGT", "Target Corporation", "87612E"),
    ("CVS", "CVS Health Corporation", "126650"),
    ("WBA", "Walgreens Boots Alliance Inc", "931427"),
    ("AXP", "American Express Company", "025816"),
    ("BLK", "BlackRock Inc", "09247X"),
    ("SCHW", "Charles Schwab Corporation", "808513"),
    ("USB", "U.S. Bancorp", "902973"),
    ("PNC", "PNC Financial Services Group", "693475"),
    ("TFC", "Truist Financial Corporation", "89832Q"),
    ("COF", "Capital One Financial Corporation", "14040H"),
    ("AIG", "American International Group", "026874"),
    ("MET", "MetLife Inc", "59156R"),
    ("PRU", "Prudential Financial Inc", "744320"),
    ("ALL", "Allstate Corporation", "020002"),
    ("TRV", "Travelers Companies Inc", "89417E"),
    ("CB", "Chubb Limited", "H1467J"),
    ("SO", "Southern Company", "842587"),
    ("DUK", "Duke Energy Corporation", "26441C"),
    ("NEE", "NextEra Energy Inc", "65339F"),
    ("D", "Dominion Energy Inc", "25746U"),
    ("AEP", "American Electric Power Company", "025537"),
    ("XEL", "Xcel Energy Inc", "98389B"),
    ("SRE", "Sempra Energy", "816851"),
    ("PCG", "PG&E Corporation", "69331C"),
    ("EXC", "Exelon Corporation", "30161N"),
    ("ED", "Consolidated Edison Inc", "209115"),
    ("WM", "Waste Management Inc", "94106L"),
    ("RSG", "Republic Services Inc", "760759"),
    ("AMT", "American Tower Corporation", "03027X"),
    ("CCI", "Crown Castle Inc", "228227"),
    ("PLD", "Prologis Inc", "74340W"),
    ("EQIX", "Equinix Inc", "29444U"),
    ("SPG", "Simon Property Group Inc", "828806"),
    ("PSA", "Public Storage", "74460D"),
    ("O", "Realty Income Corporation", "756109"),
    ("WELL", "Welltower Inc", "95040Q"),
    ("AVB", "AvalonBay Communities Inc", "053484"),
    ("EQR", "Equity Residential", "29476L"),
    ("MAA", "Mid-America Apartment Communities", "59522J"),
    ("KMI", "Kinder Morgan Inc", "49456B"),
    ("WMB", "Williams Companies Inc", "969457"),
]

CREDIT_RATINGS = [
    ("AAA", 1), ("AA+", 2), ("AA", 3), ("AA-", 4),
    ("A+", 5), ("A", 6), ("A-", 7),
    ("BBB+", 8), ("BBB", 9), ("BBB-", 10),
    ("BB+", 11), ("BB", 12), ("BB-", 13),
    ("B+", 14), ("B", 15), ("B-", 16),
]

def generate_cusip(base_cusip, issue_num):
    return f"{base_cusip}{issue_num:02d}0"

def generate_isin(cusip):
    return f"US{cusip}0"

def generate_figi():
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "BBG" + "".join(random.choices(chars, k=9))

def generate_bonds():
    bonds = []
    bond_id = 1
    
    for ticker, issuer_name, base_cusip in TOP_CORPORATE_ISSUERS:
        num_bonds = random.randint(8, 15)
        
        if ticker in ["AAPL", "MSFT", "GOOG", "AMZN", "JPM", "BAC"]:
            rating_idx = random.randint(0, 3)
        elif ticker in ["META", "NVDA", "GS", "MS", "V", "MA"]:
            rating_idx = random.randint(2, 6)
        elif ticker in ["F", "GM", "PCG"]:
            rating_idx = random.randint(8, 12)
        else:
            rating_idx = random.randint(4, 9)
        
        for i in range(num_bonds):
            cusip = generate_cusip(base_cusip, i + 1)
            isin = generate_isin(cusip)
            figi = generate_figi()
            
            issue_year = random.randint(2015, 2025)
            maturity_years = random.choice([3, 5, 7, 10, 20, 30])
            issue_date = datetime(issue_year, random.randint(1, 12), random.randint(1, 28))
            maturity_date = issue_date + timedelta(days=maturity_years * 365)
            
            if issue_year <= 2020:
                base_coupon = random.uniform(1.5, 4.5)
            elif issue_year <= 2022:
                base_coupon = random.uniform(2.0, 4.0)
            else:
                base_coupon = random.uniform(4.0, 6.5)
            
            coupon_rate = round(base_coupon + rating_idx * 0.15, 3)
            
            if maturity_date > datetime.now():
                yield_adjustment = random.uniform(-0.5, 1.0)
                current_yield = round(coupon_rate + yield_adjustment, 3)
            else:
                current_yield = None
            
            rating, _ = CREDIT_RATINGS[min(rating_idx + random.randint(-1, 1), len(CREDIT_RATINGS) - 1)]
            
            par_value = random.choice([500, 750, 1000, 1250, 1500, 2000, 2500, 3000]) * 1000000
            
            bonds.append({
                'bond_id': bond_id,
                'cusip': cusip,
                'isin': isin,
                'figi': figi,
                'ticker': ticker,
                'issuer_name': issuer_name,
                'issue_date': issue_date.strftime('%Y-%m-%d'),
                'maturity_date': maturity_date.strftime('%Y-%m-%d'),
                'coupon_rate': coupon_rate,
                'coupon_frequency': random.choice(['SEMI-ANNUAL', 'QUARTERLY', 'ANNUAL']),
                'current_yield': current_yield,
                'credit_rating': rating,
                'par_value': par_value,
                'currency': 'USD',
                'bond_type': random.choice(['SENIOR UNSECURED', 'SENIOR SECURED', 'SUBORDINATED']),
                'callable': random.choice([True, False]),
                'sector': get_sector(ticker),
            })
            bond_id += 1
    
    return bonds

def get_sector(ticker):
    sectors = {
        'AAPL': 'Technology', 'MSFT': 'Technology', 'GOOG': 'Technology', 'META': 'Technology',
        'AMZN': 'Consumer Discretionary', 'NVDA': 'Technology', 'AMD': 'Technology',
        'JPM': 'Financials', 'BAC': 'Financials', 'WFC': 'Financials', 'C': 'Financials',
        'GS': 'Financials', 'MS': 'Financials', 'V': 'Financials', 'MA': 'Financials',
        'XOM': 'Energy', 'CVX': 'Energy', 'KMI': 'Energy', 'WMB': 'Energy',
        'PFE': 'Healthcare', 'JNJ': 'Healthcare', 'UNH': 'Healthcare', 'MRK': 'Healthcare',
        'ABBV': 'Healthcare', 'TMO': 'Healthcare', 'ABT': 'Healthcare',
        'HD': 'Consumer Discretionary', 'WMT': 'Consumer Staples', 'COST': 'Consumer Staples',
        'PG': 'Consumer Staples', 'KO': 'Consumer Staples', 'PEP': 'Consumer Staples',
        'DIS': 'Communication Services', 'VZ': 'Communication Services', 'T': 'Communication Services',
        'CMCSA': 'Communication Services', 'INTC': 'Technology', 'IBM': 'Technology',
        'ORCL': 'Technology', 'CRM': 'Technology', 'CSCO': 'Technology', 'ACN': 'Technology',
        'TXN': 'Technology', 'QCOM': 'Technology', 'CAT': 'Industrials', 'DE': 'Industrials',
        'BA': 'Industrials', 'GE': 'Industrials', 'HON': 'Industrials', 'MMM': 'Industrials',
        'UPS': 'Industrials', 'LMT': 'Industrials', 'RTX': 'Industrials',
        'F': 'Consumer Discretionary', 'GM': 'Consumer Discretionary', 'TM': 'Consumer Discretionary',
        'TSLA': 'Consumer Discretionary', 'NKE': 'Consumer Discretionary', 'MCD': 'Consumer Discretionary',
        'SBUX': 'Consumer Discretionary', 'LOW': 'Consumer Discretionary', 'TGT': 'Consumer Discretionary',
        'CVS': 'Healthcare', 'WBA': 'Healthcare', 'AXP': 'Financials', 'BLK': 'Financials',
        'SCHW': 'Financials', 'USB': 'Financials', 'PNC': 'Financials', 'TFC': 'Financials',
        'COF': 'Financials', 'AIG': 'Financials', 'MET': 'Financials', 'PRU': 'Financials',
        'ALL': 'Financials', 'TRV': 'Financials', 'CB': 'Financials',
        'SO': 'Utilities', 'DUK': 'Utilities', 'NEE': 'Utilities', 'D': 'Utilities',
        'AEP': 'Utilities', 'XEL': 'Utilities', 'SRE': 'Utilities', 'PCG': 'Utilities',
        'EXC': 'Utilities', 'ED': 'Utilities', 'WM': 'Industrials', 'RSG': 'Industrials',
        'AMT': 'Real Estate', 'CCI': 'Real Estate', 'PLD': 'Real Estate', 'EQIX': 'Real Estate',
        'SPG': 'Real Estate', 'PSA': 'Real Estate', 'O': 'Real Estate', 'WELL': 'Real Estate',
        'AVB': 'Real Estate', 'EQR': 'Real Estate', 'MAA': 'Real Estate',
    }
    return sectors.get(ticker, 'Other')

def main():
    print("Generating corporate bond data...")
    bonds = generate_bonds()
    print(f"Generated {len(bonds)} bonds")
    
    bonds.sort(key=lambda x: x['par_value'], reverse=True)
    top_1000 = bonds[:1000]
    
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "colms_uswest")
    cursor = conn.cursor()
    
    cursor.execute("USE DATABASE SECURITIES_MASTER")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS FIXED_INCOME")
    cursor.execute("USE SCHEMA FIXED_INCOME")
    
    cursor.execute("""
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
        )
    """)
    
    print("Inserting bond data into Snowflake...")
    
    insert_sql = """
        INSERT INTO CORPORATE_BONDS (
            BOND_ID, CUSIP, ISIN, FIGI, TICKER, ISSUER_NAME, 
            ISSUE_DATE, MATURITY_DATE, COUPON_RATE, COUPON_FREQUENCY,
            CURRENT_YIELD, CREDIT_RATING, PAR_VALUE, CURRENCY,
            BOND_TYPE, CALLABLE, SECTOR
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for bond in top_1000:
        cursor.execute(insert_sql, (
            bond['bond_id'],
            bond['cusip'],
            bond['isin'],
            bond['figi'],
            bond['ticker'],
            bond['issuer_name'],
            bond['issue_date'],
            bond['maturity_date'],
            bond['coupon_rate'],
            bond['coupon_frequency'],
            bond['current_yield'],
            bond['credit_rating'],
            bond['par_value'],
            bond['currency'],
            bond['bond_type'],
            bond['callable'],
            bond['sector'],
        ))
    
    cursor.execute("SELECT COUNT(*) FROM CORPORATE_BONDS")
    count = cursor.fetchone()[0]
    print(f"\nInserted {count} bonds into SECURITIES_MASTER.FIXED_INCOME.CORPORATE_BONDS")
    
    print("\nSample data:")
    cursor.execute("""
        SELECT CUSIP, ISIN, TICKER, ISSUER_NAME, COUPON_RATE, MATURITY_DATE, CREDIT_RATING, PAR_VALUE
        FROM CORPORATE_BONDS 
        ORDER BY PAR_VALUE DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(row)
    
    print("\nBonds by sector:")
    cursor.execute("""
        SELECT SECTOR, COUNT(*) as COUNT, SUM(PAR_VALUE) as TOTAL_PAR_VALUE
        FROM CORPORATE_BONDS
        GROUP BY SECTOR
        ORDER BY TOTAL_PAR_VALUE DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} bonds, ${row[2]:,.0f} par value")
    
    print("\nBonds by credit rating:")
    cursor.execute("""
        SELECT CREDIT_RATING, COUNT(*) as COUNT
        FROM CORPORATE_BONDS
        GROUP BY CREDIT_RATING
        ORDER BY CREDIT_RATING
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} bonds")
    
    cursor.close()
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
