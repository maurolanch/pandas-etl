import pandas as pd
import glob, os
from typing import Dict

# Verifying that the downloaded CSV files exist in the data/ folder
def verify_files(data_dir: str = "data") -> list:
    """
    Verify that CSV files exist in the directory.
    """
    pattern = os.path.join(data_dir, 'ecommerce_*.csv')
    files = glob.glob(pattern)

    if not files:
        print("Files not found. Make sure to download them into the data/ folder.")
        print("   You should have: ecommerce_orders.csv, ecommerce_customers.csv, etc.")
    else:
        print(f"Files found: {len(files)}")
        for f in sorted(files):
            print(f"  - {os.path.basename(f)}")
    return files

def load_csv(filepath: str) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame.
    """
    try:
        df = pd.read_csv(filepath)
        print(f"Loaded {os.path.basename(filepath)} with {len(df)} rows and {len(df.columns)} columns.")
        return df
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def load_all_tables(data_dir: str = "data") -> Dict[str, pd.DataFrame]:
    """
    Load all necessary tables for the pipeline.
    """
    tables = {
        "orders": "ecommerce_orders.csv",
        "order_items": "ecommerce_order_items.csv",
        "customers": "ecommerce_customers.csv",
        "products": "ecommerce_products.csv",
    }

    dataframes = {}

    for name, filename in tables.items():
        filepath = os.path.join(data_dir, filename)
        dataframes[name] = load_csv(filepath)

    return dataframes


def transform_data(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Perform data cleaning and transformation.
    """
    df_orders = tables["orders"].copy()
    df_order_items = tables["order_items"].copy()
    df_customers = tables["customers"].copy()  
    df_products = tables["products"].copy()  
    # 1. Handling null values
    df_orders = df_orders.dropna(subset=['order_id', 'customer_id', 'total_amount'])    
    
    # 2. Fixing data types
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce')
    df_orders['total_amount'] = pd.to_numeric(df_orders['total_amount'], errors='coerce')

    # 3. Removing duplicates based on primary keys
    # --- Orders (PK: order_id)
    df_orders = (
        df_orders
        .sort_values('order_date')
        .drop_duplicates(subset=['order_id'], keep='last')
    )

    # --- Order items (PK compuesta: order_id + product_id)
    df_order_items = (
        df_order_items
        .drop_duplicates(subset=['order_id', 'product_id'])
    )
    # 4. Joining tables to create a denormalized view for analysis
    df = df_orders.merge(df_order_items, on="order_id", how="left")

    df_customers["customer_name"] = (
        df_customers["first_name"] + " " + df_customers["last_name"]
    )

    df = df.merge(df_customers[["customer_id", "customer_name"]], 
                  on="customer_id", how="left")
    df = df.merge(df_products[["product_id", "product_name"]], 
                  on="product_id", how="left")

    # 5. Adding a new column for month of the order
    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)

    return df


def load(df: pd.DataFrame, output_dir: str = "output"):
    print("\n LOAD")

    os.makedirs(output_dir, exist_ok=True)

    # 1. Top 5 spenders
    top_customers = (
        df.groupby(["customer_id", "customer_name"])["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    top_customers.to_csv(f"{output_dir}/top_customers.csv", index=False)
    top_customers.to_parquet(f"{output_dir}/top_customers.parquet", index=False)

    # 2. Best-selling product (by quantity)
    top_product = (
        df.groupby(["product_id", "product_name"])["quantity"]
        .sum()
        .sort_values(ascending=False)
        .head(1)
        .reset_index()
    )

    top_product.to_csv(f"{output_dir}/top_product.csv", index=False)
    top_product.to_parquet(f"{output_dir}/top_product.parquet", index=False)

    # 3. Sales by month
    sales_by_month = (
        df.groupby("order_month")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("order_month")
    )

    sales_by_month.to_csv(f"{output_dir}/sales_by_month.csv", index=False)
    sales_by_month.to_parquet(f"{output_dir}/sales_by_month.parquet", index=False)

    print("Metrics saved in output/ folder.")

def main():
    verify_files()

    tables = load_all_tables()

    df = transform_data(tables)

    load(df)


if __name__ == "__main__":
    main()