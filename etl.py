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

print("\n Nulls - orders:")
print(df_orders.isnull().sum())

print("\n Nulls - order_items:")
print(df_order_items.isnull().sum())

print("\n Nulls - customers:")
print(df_customers.isnull().sum())

print("\n Nulls - products:")
print(df_products.isnull().sum())


# 1. COPY to create df_orders_clean, so we can keep the original df_orders intact for reference
df_orders_clean = df_orders.copy()

# 2. fix data types

# Date
df_orders_clean['order_date'] = pd.to_datetime(df_orders_clean['order_date'], errors='coerce')

# Numeric (just in case there are some non-numeric values, we set errors='coerce' to convert them to NaN)
df_orders_clean['total_amount'] = pd.to_numeric(df_orders_clean['total_amount'], errors='coerce')
df_orders_clean['shipping_cost'] = pd.to_numeric(df_orders_clean['shipping_cost'], errors='coerce')

# String
df_orders_clean['order_number'] = df_orders_clean['order_number'].astype("string")

# 3. handle missing values

# critical fields: order_id, customer_id, total_amount - we cannot have missing values here, so we drop those rows
df_orders_clean = df_orders_clean.dropna(subset=['order_id', 'customer_id', 'total_amount'])

# optional fields: shipping_cost, discount_percent - we can fill missing values with 0, assuming no shipping cost or no discount
df_orders_clean['discount_percent'] = df_orders_clean['discount_percent'].fillna(0)


# 4. duplicates

# complete duplicates
duplicates = df_orders_clean.duplicated().sum()
print(f"Found duplicates: {duplicates}")

# duplicates per pk
duplicates_id = df_orders_clean.duplicated(subset=['order_id']).sum()
print(f"Order_id Duplicates: {duplicates_id}")

# drop complete duplicates
df_orders_clean = df_orders_clean.drop_duplicates()

# keep unique order_id, if there are duplicates, we keep the last one (assuming it's the most updated record)
df_orders_clean = df_orders_clean.sort_values('order_date').drop_duplicates(
    subset=['order_id'], 
    keep='last'
)

# 5. final check

print("\n Data types after conversion:")
print(df_orders_clean.dtypes)

print(f"\nRows before: {len(df_orders)}, rows after: {len(df_orders_clean)}")