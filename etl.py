import pandas as pd
import glob, os

# Verifying that the downloaded CSV files exist
files = glob.glob('data/ecommerce_*.csv')
if not files:
    print("❌ Files not found. Make sure to download them into the data/ folder.")
    print("   You should have: ecommerce_orders.csv, ecommerce_customers.csv, etc.")
else:
    print(f"📂 Archivos encontrados: {len(files)}")
    for f in sorted(files):
        print(f"  - {os.path.basename(f)}")


# Load main CSVs into DataFrames
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

# Exploring the data
print(f"\n Summary:")
print(f"Orders: {len(df_orders)} filas, {len(df_orders.columns)} columns")
print(f"Order Items: {len(df_order_items)} rows")
print(f"Customers: {len(df_customers)} rows")
print(f"Products: {len(df_products)} rows")