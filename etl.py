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

print("\n First rows from orders")
print(df_orders.head())
print("\n Orders info:")
print(df_orders.info())

print("\n Nulls - order_items:")
print(df_order_items.isnull().sum())

print("\n Nulls - customers:")
print(df_customers.isnull().sum())

print("\n Nulls - products:")
print(df_products.isnull().sum())