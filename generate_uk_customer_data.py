"""
Generate realistic UK customer web analytics data as JSON files
for Snowpipe Streaming ingestion
"""

import json
import random
from datetime import datetime, timedelta
import os

# UK-specific data for realistic generation
UK_CITIES_IPS = {
    "London": ["185.86.", "194.168.", "212.58.", "31.52."],
    "Manchester": ["81.105.", "82.132.", "109.170.", "86.156."],
    "Birmingham": ["92.233.", "86.149.", "109.157.", "81.103."],
    "Leeds": ["90.216.", "86.146.", "109.154.", "81.108."],
    "Glasgow": ["92.238.", "86.159.", "109.148.", "81.111."],
    "Liverpool": ["81.100.", "86.147.", "109.171.", "90.218."],
    "Bristol": ["86.153.", "109.155.", "81.106.", "92.234."],
    "Edinburgh": ["86.158.", "109.149.", "81.112.", "92.239."],
    "Cardiff": ["86.154.", "109.156.", "81.107.", "92.235."],
    "Belfast": ["86.160.", "109.150.", "81.113.", "92.240."],
}

UK_SEARCH_PHRASES = [
    "best fish and chips near me",
    "weather forecast london",
    "premier league results",
    "train times to manchester",
    "nhs appointment booking",
    "uk visa requirements",
    "cheap flights heathrow",
    "rightmove houses for sale",
    "bbc news live",
    "uk bank holidays 2026",
    "tesco delivery slots",
    "argos click and collect",
    "currys pc world laptops",
    "boots photo printing",
    "next day delivery uk",
    "john lewis sale",
    "marks and spencer food",
    "sainsburys groceries",
    "amazon uk prime",
    "ebay uk",
    "royal mail tracking",
    "dvla tax check",
    "council tax bands",
    "uk passport renewal",
    "energy price cap uk",
    "mortgage rates uk",
    "isa allowance 2026",
    "national lottery results",
    "sky sports football",
    "bt broadband deals",
]

UK_PAGE_TITLES = [
    "BBC News - Home",
    "The Guardian | News, Sport and Opinion",
    "Daily Mail Online - Latest News",
    "Sky News - First For Breaking News",
    "Rightmove - UK Property Search",
    "Amazon.co.uk: Low Prices in Electronics",
    "eBay UK | Electronics, Cars, Fashion",
    "Tesco Groceries :: Online Food Shopping",
    "Sainsbury's - Online Grocery Shopping",
    "Argos | Same Day Delivery",
    "John Lewis & Partners",
    "Marks & Spencer | Clothing & Home",
    "Next Official Site: Online Fashion",
    "ASOS | Online Shopping for Clothes",
    "Boots UK | Health & Beauty",
    "Currys | Electricals, TVs, Laptops",
    "Halfords | Bikes, Car Parts & Accessories",
    "National Rail Enquiries",
    "Premier League Football News",
    "NHS - Health A-Z",
    "GOV.UK - Government Services",
    "HMRC - Tax Services",
    "Zoopla | Property for Sale and Rent",
    "AutoTrader UK - New & Used Cars",
    "Compare the Market",
    "MoneySuperMarket",
    "Booking.com - Hotels in UK",
    "British Airways | Book Flights",
    "Trainline - Train Tickets UK",
    "Deliveroo - Food Delivery",
]

# Search Engine IDs (common UK search engines)
SEARCH_ENGINES = {
    1: "Google UK",
    2: "Bing UK",
    3: "Yahoo UK",
    4: "DuckDuckGo",
    5: "Ecosia",
    0: "Direct/None",
}

# Common screen resolutions
RESOLUTIONS = [1920, 1366, 1536, 1440, 1280, 2560, 3840, 1680, 1600, 1024]


def generate_uk_ip():
    """Generate a realistic UK IP address"""
    city = random.choice(list(UK_CITIES_IPS.keys()))
    prefix = random.choice(UK_CITIES_IPS[city])
    return f"{prefix}{random.randint(1, 254)}.{random.randint(1, 254)}"


def generate_customer_event(event_date):
    """Generate a single customer event record"""
    return {
        "EVENTDATE": event_date.strftime("%Y-%m-%d"),
        "COUNTERID": random.randint(100000, 999999),
        "CLIENTIP": generate_uk_ip(),
        "SEARCHENGINEID": random.choices(
            list(SEARCH_ENGINES.keys()), 
            weights=[50, 20, 10, 10, 5, 5]  # Google most common
        )[0],
        "SEARCHPHRASE": random.choice(UK_SEARCH_PHRASES) if random.random() > 0.3 else "",
        "RESOLUTIONWIDTH": random.choice(RESOLUTIONS),
        "TITLE": random.choice(UK_PAGE_TITLES),
        "ISREFRESH": random.choices([0, 1], weights=[85, 15])[0],
        "DONTCOUNTHITS": random.choices([0, 1], weights=[95, 5])[0],
    }


def generate_batch(batch_size=100, days_back=30):
    """Generate a batch of customer events"""
    events = []
    base_date = datetime.now()
    
    for _ in range(batch_size):
        # Random date within the range
        event_date = base_date - timedelta(days=random.randint(0, days_back))
        events.append(generate_customer_event(event_date))
    
    return events


def save_json_files(output_dir="json_data", num_files=5, records_per_file=100):
    """Generate and save JSON files"""
    os.makedirs(output_dir, exist_ok=True)
    
    files_created = []
    for i in range(num_files):
        events = generate_batch(records_per_file)
        filename = f"{output_dir}/uk_customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i+1}.json"
        
        with open(filename, 'w') as f:
            json.dump(events, f, indent=2)
        
        files_created.append(filename)
        print(f"Created: {filename} ({len(events)} records)")
    
    return files_created


if __name__ == "__main__":
    print("Generating UK Customer Data JSON Files...")
    print("=" * 50)
    
    # Generate 5 files with 100 records each (500 total)
    files = save_json_files(
        output_dir="json_data",
        num_files=5,
        records_per_file=100
    )
    
    print("=" * 50)
    print(f"Generated {len(files)} JSON files")
    print("\nSample record:")
    sample = generate_customer_event(datetime.now())
    print(json.dumps(sample, indent=2))
