import pandas as pd
from datetime import datetime
import os
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib

load_dotenv()

# ================== Config ==================
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

if not all([SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD]):
    raise ValueError("Missing SQL credentials in .env file")

# ================== Generate UNIQUE Customer Data ==================
def generate_customers(num_records=50):
    regions = ["North", "South", "East", "West", "Central"]
    channels = ["Retail", "Online", "Wholesale", "Direct", "Partner"]
    
    first_names = ["Rahul", "Priya", "Amit", "Sneha", "Vikram", "Neha", "Arjun", "Meera", "Rohan", "Ananya"]
    last_names = ["Sharma", "Patel", "Singh", "Kumar", "Reddy", "Gupta", "Mehta", "Joshi", "Nair", "Bansal"]
    
    data = []
    
    # Use a set to ensure uniqueness
    used_ids = set()
    
    while len(data) < num_records:
        # Generate a random customer_id (more realistic)
        customer_id = f"C{random.randint(10000, 99999)}"
        
        if customer_id in used_ids:
            continue
            
        used_ids.add(customer_id)
        
        customer_name = f"{random.choice(first_names)} {random.choice(last_names)}"
        
        data.append({
            "customer_id": customer_id,
            "customer_name": customer_name,
            "region": random.choice(regions),
            "channel": random.choice(channels),
            "last_modified": datetime.now()
        })
    
    return pd.DataFrame(data)


print("Generating unique customer data...")
df = generate_customers(num_records=80)

print(f"Generated {len(df)} unique customer records")

# ================== Connection String (with relaxed settings) ==================
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=90;"
    "Login Timeout=90;"
)

try:
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    df.to_sql(
        name="Customer",
        schema="dbo",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500
    )

    print(f"✅ SUCCESS! {len(df)} new customer records inserted into dbo.Customer")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")