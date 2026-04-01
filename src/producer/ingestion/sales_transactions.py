import pandas as pd
from datetime import datetime, timedelta
import os
import random
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ================== Configuration from .env ==================
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME       = os.getenv("CONTAINER_NAME", "raw")
FOLDER_PATH          = os.getenv("FOLDER_PATH", "sales/transactions/")
ACCOUNT_KEY          = os.getenv("STORAGE_ACCOUNT_KEY")

if not STORAGE_ACCOUNT_NAME or not ACCOUNT_KEY:
    raise ValueError("❌ Missing STORAGE_ACCOUNT_NAME or STORAGE_ACCOUNT_KEY in .env file")

# ===================================================

# ================== Generate Dynamic Sales Data ==================

def generate_dynamic_sales_data(num_records=50):
    """Generate realistic dynamic sales transaction data"""
    
    # Sample pools for realistic data
    customer_ids = [f"C{100 + i}" for i in range(50)]
    product_ids = [f"P{1000 + i}" for i in range(30)]
    currencies = ["JPY", "USD", "EUR", "INR"]
    source_systems = ["SAP"]
    
    data = []
    
    # Base date - today
    base_date = datetime.now().date()
    
    for i in range(num_records):
        # Dynamic order_date: last 30 days (more realistic)
        days_offset = random.randint(0, 30)
        order_date = (base_date - timedelta(days=days_offset)).strftime("%Y-%m-%d")
        
        sales_id = f"S{str(10000 + i).zfill(4)}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 50)
        
        # Generate realistic net_amount
        unit_price = round(random.uniform(100, 5000), 2)
        net_amount = round(quantity * unit_price, 2)
        
        # Occasionally add negative (returns)
        if random.random() < 0.08:  # 8% chance of return
            net_amount = -abs(net_amount)
        
        currency = random.choice(currencies)
        source_system = random.choice(source_systems)
        
        data.append({
            "sales_id": sales_id,
            "order_date": order_date,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "net_amount": net_amount,
            "currency": currency,
            "source_system": source_system
        })
    
    return pd.DataFrame(data)

# Generate dynamic data
df = generate_dynamic_sales_data(num_records=100)   # Change number as needed

# ================== File Naming ==================
current_date = datetime.now().strftime("%Y%m%d")
file_name = f"sales_transactions_{current_date}.csv"

file_path = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{FOLDER_PATH}{file_name}"

print(f"📊 Generated {len(df)} dynamic sales transactions")
print(f"📅 Date Range : {df['order_date'].min()} to {df['order_date'].max()}")
print(f"📤 Writing file: {file_name}\n")

# ================ Write to ADLS Gen2 =================
try:
    df.to_csv(
        file_path,
        index=False,
        storage_options={"account_key": ACCOUNT_KEY}
    )
    
    print(f"✅ SUCCESS! File uploaded to ADLS Gen2")
    print(f"   File Name     : {file_name}")
    print(f"   Rows Written  : {len(df)}")
    print(f"   Date Range    : {df['order_date'].min()} → {df['order_date'].max()}")
    print(f"   Location      : {FOLDER_PATH}{file_name}")

except Exception as e:
    print(f"❌ ERROR: Failed to write file to ADLS Gen2")
    print(f"   Error: {str(e)}")
    if "Protocol not known: abfss" in str(e):
        print("\n💡 Hint: pip install --upgrade pandas fsspec adlfs")