import pandas as pd
import glob, os

# Verifying that the downloaded CSV files exist
files = glob.glob('data/ecommerce_*.csv')
if not files:
    print("❌ Files not found. Make sure to download them into the data/ folder.")
    print("   You should have: ecommerce_orders.csv, ecommerce_customers.csv, etc.")
else:
    print(f"📂 Files found: {len(files)}")
    for f in sorted(files):
        print(f"  - {os.path.basename(f)}")


# Load main CSVs into DataFrames
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

# Exploring the data
print(f"\n Summary:")
print(f"Orders: {len(df_orders)} rows, {len(df_orders.columns)} columns")
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

# 1. Who are the top 5 spenders?

top_spenders = (
    df_orders_clean
    .groupby('customer_id', as_index=False)
    .agg(
        total_amount=('total_amount', 'sum'),
        order_count=('order_id', 'count')
    )
    .sort_values(by='total_amount', ascending=False)
    .head(5)
)
print("\n Top 5 spenders:")
print(top_spenders)

# 2. What is the best-selling product (by quantity)?

best_selling_product = (
    df_order_items
    .groupby('product_id', as_index=False)[['quantity']]
    .sum()
    .sort_values(by='quantity', ascending=False)
    .head(1)
    .merge(df_products[['product_id', 'product_name']], on='product_id', how='left')
)
print("\n Best-selling product:")
print(best_selling_product)

#  3. How did sales evolve month by month?

df_orders_clean['month'] = df_orders_clean['order_date'].dt.to_period('M')

sales_by_month = (
    df_orders_clean
    .groupby('month', as_index=False)
    .agg(total_sales=('total_amount', 'sum'))
    .sort_values(by='month')
)
print("\n Sales by month:") 
print(sales_by_month)

print(df_orders_clean)

#Saving the cleaned orders data to a new CSV file
df_orders_clean.to_csv('output/ecommerce_orders_clean.csv', index=False)

#saving top 5 spenders to a new CSV file
top_spenders.to_csv('output/top_spenders.csv', index=False)

#saving sales by month to a new CSV file
sales_by_month.to_csv('output/sales_by_month.csv', index=False)

#saving best selling product to a new CSV file
best_selling_product.to_csv('output/best_selling_product.csv', index=False)

#saving the cleaned orders data to a parquet file
df_orders_clean.to_parquet('output/ecommerce_orders_clean.parquet', index=False)

#saving top 5 spenders to a parquet file
top_spenders.to_parquet('output/top_spenders.parquet', index=False)

#saving sales by month to a parquet file
sales_by_month.to_parquet('output/sales_by_month.parquet', index=False)

#saving best selling product to a parquet file
best_selling_product.to_parquet('output/best_selling_product.parquet', index=False)
