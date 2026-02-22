"""
Snowpipe Streaming - Ingest SDK
For populating Interactive Tables in real-time

Prerequisites:
    pip install snowflake-ingest cryptography

This is the ONLY way to insert data into Interactive Tables.
Regular SQL INSERT/COPY commands do NOT work!
"""

import json
import os
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Check if snowflake-ingest streaming is available
try:
    from snowflake.ingest import SnowflakeStreamingIngestClient
    STREAMING_SDK_AVAILABLE = True
except ImportError:
    STREAMING_SDK_AVAILABLE = False
    print("Note: snowflake-ingest not installed. Run: pip install snowflake-ingest")


# ============================================
# CONFIGURATION
# ============================================
CONFIG = {
    "account": "your_account",  # e.g., "SFSEEUROPE-COLM_USWEST"
    "user": "your_user",
    "private_key_path": os.path.expanduser("~/.ssh/snowflake/rsa_key.p8"),
    "private_key_passphrase": None,  # Set if your key is encrypted
    "database": "INTERACTIVE_JSON_DB",
    "schema": "STREAMING",
    "table": "CUSTOMERS",
    "role": "ACCOUNTADMIN",
}


def load_private_key(path: str, passphrase: str = None):
    """Load private key for Snowflake authentication"""
    with open(path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
    return private_key


def stream_json_to_interactive_table(json_file_path: str, table_name: str = None):
    """
    Stream JSON data directly to an Interactive Table
    
    For Interactive Tables, you MUST use the Snowflake Ingest SDK.
    Regular SQL INSERT/COPY does not work!
    
    Args:
        json_file_path: Path to JSON file containing records
        table_name: Target table name (defaults to CONFIG["table"])
    """
    
    if not STREAMING_SDK_AVAILABLE:
        print("""
    ============================================
    SNOWFLAKE INGEST SDK REQUIRED
    ============================================
    
    Interactive Tables can ONLY be populated via:
    1. Snowflake Ingest SDK (this script)
    2. Snowflake Kafka Connector
    3. Snowflake Spark Connector (streaming mode)
    
    To install the SDK:
        pip install snowflake-ingest cryptography
    
    ============================================
        """)
        return
    
    table_name = table_name or CONFIG["table"]
    
    # Load JSON data
    with open(json_file_path, 'r') as f:
        records = json.load(f)
    
    print(f"📂 Loaded {len(records)} records from {json_file_path}")
    
    try:
        # Load private key
        private_key = load_private_key(
            CONFIG["private_key_path"], 
            CONFIG["private_key_passphrase"]
        )
        
        # Create streaming client
        client = SnowflakeStreamingIngestClient(
            account=CONFIG["account"],
            user=CONFIG["user"],
            private_key=private_key,
            role=CONFIG["role"],
        )
        
        # Open a channel to the Interactive Table
        channel = client.open_channel(
            name=f"{table_name}_channel",
            database_name=CONFIG["database"],
            schema_name=CONFIG["schema"],
            table_name=table_name,
        )
        
        # Stream records to the Interactive Table
        print(f"🚀 Streaming to {CONFIG['database']}.{CONFIG['schema']}.{table_name}...")
        
        for i, record in enumerate(records):
            channel.insert_row(record)
            
            if (i + 1) % 100 == 0:
                print(f"   Streamed {i + 1}/{len(records)} records...")
        
        # Flush and close
        channel.close()
        
        print(f"✅ Successfully streamed {len(records)} records to Interactive Table: {table_name}")
        
    except FileNotFoundError:
        print(f"""
    ❌ Private key not found at: {CONFIG['private_key_path']}
    
    To set up keypair authentication:
    1. Generate a key pair:
       openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
       openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    
    2. Assign public key to your Snowflake user:
       ALTER USER {CONFIG['user']} SET RSA_PUBLIC_KEY='<public_key_content>';
    
    3. Update CONFIG['private_key_path'] to point to your private key file.
        """)
        
    except Exception as e:
        print(f"❌ Error streaming data: {e}")


def demo_interactive_table():
    """
    Demo using Interactive Table with Ingest SDK
    """
    
    print("""
    ============================================
    INTERACTIVE TABLE STREAMING DEMO
    ============================================
    
    This demo streams data to:
    
    Interactive Table: CUSTOMERS
    - Real-time analytics with sub-second latency
    - Populated via Snowflake Ingest SDK only
    
    Data flow:
    JSON Files -> Ingest SDK -> Interactive Table CUSTOMERS
    
    To stream data:
    
        from snowpipe_streaming_ingest import stream_json_to_interactive_table
        stream_json_to_interactive_table("json_data/customers.json")
    
    To query data:
    
        SELECT * FROM CUSTOMERS LIMIT 100;
        
        -- By UK city (based on IP prefix)
        SELECT 
            CASE 
                WHEN CLIENTIP LIKE '185.86.%' OR CLIENTIP LIKE '31.52.%' THEN 'London'
                WHEN CLIENTIP LIKE '81.105.%' OR CLIENTIP LIKE '82.132.%' THEN 'Manchester'
                WHEN CLIENTIP LIKE '92.233.%' OR CLIENTIP LIKE '86.149.%' THEN 'Birmingham'
                ELSE 'Other UK'
            END AS CITY,
            COUNT(*) as VISITORS
        FROM CUSTOMERS
        GROUP BY 1
        ORDER BY 2 DESC;
    
    ============================================
    """)


if __name__ == "__main__":
    demo_interactive_table()
    
    # If you have JSON files to stream
    if os.path.exists("json_data"):
        json_files = [f for f in os.listdir("json_data") if f.endswith(".json")]
        
        if json_files:
            print(f"\n📁 Found {len(json_files)} JSON files in json_data/")
            print("To stream them to the Interactive Table, run:")
            print(f'    stream_json_to_interactive_table("json_data/{json_files[0]}")')
