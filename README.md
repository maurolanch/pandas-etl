# 📊 E-commerce ETL Pipeline (Pandas)

This project implements a simple **ETL (Extract, Transform, Load)** pipeline using Python and Pandas to process e-commerce data.

It cleans raw CSV files, performs data quality checks, generates business insights, and exports results in both **CSV and Parquet formats**.

---

## 🚀 Overview

The pipeline processes the following datasets:

- `ecommerce_orders.csv`
- `ecommerce_order_items.csv`
- `ecommerce_customers.csv`
- `ecommerce_products.csv`

### 🔄 ETL Flow

1. **Extract**
   - Load raw CSV files from the `/data` folder

2. **Transform**
   - Data type standardization
   - Null handling
   - Duplicate removal:
     - Orders deduplicated by `order_id` (keeping latest record)
     - Order items deduplicated by (`order_id`, `product_id`)
   - Data enrichment:
     - Join with customers and products
     - Creation of `customer_name` (first + last name)
   - Feature engineering:
     - `order_month` derived from `order_date`

3. **Load**
   - Export aggregated business metrics into `/output`:
     - CSV
     - Parquet

## 📁 Project Structure

```bash
project/
├── data/
│   ├── ecommerce_orders.csv
│   ├── ecommerce_order_items.csv
│   ├── ecommerce_customers.csv
│   └── ecommerce_products.csv
│
├── output/
│   ├── ecommerce_orders_clean.csv
│   ├── top_spenders.csv
│   ├── sales_by_month.csv
│   ├── best_selling_product.csv
│   └── *.parquet
│
├── etl.py
└── README.md
```

## ⚙️ Installation

Create a virtual environment and install dependencies:


pip install pandas pyarrow


> `pyarrow` is required for Parquet export.

---

## ▶️ Usage

Run the ETL script:


python etl.py


---

## 🧹 Data Cleaning Steps

### 1. Data Type Fixing

- `order_date` → datetime  
- `total_amount`, `shipping_cost` → numeric  
- `order_number` → string  

### 2. Handling Missing Values

- Drop rows where:
  - `order_id`
  - `customer_id`
  - `total_amount`

- Fill optional fields:
  - `discount_percent = 0`

### 3. Duplicates Handling

- Remove full duplicates  
- Deduplicate by `order_id` keeping the latest record  

---

## 📈 Business Insights

### 🥇 Top 5 Customers

- Customers ranked by total spending  
- Includes:
  - `customer_id`
  - `customer_name`
  - total amount spent  

---

### 📦 Best-Selling Product

- Product with highest quantity sold  
- Includes:
  - `product_id`
  - `product_name`

---

### 📅 Sales by Month

- Monthly aggregation of total sales
- Based on:
  - `order_month`
  - `total_amount`

---

## 💾 Outputs

All results are saved in both **CSV** and **Parquet** formats:

| File | Description |
|------|------------|
| `top_customers` | Top 5 customers by spend |
| `sales_by_month` | Monthly sales trends |
| `top_product` | Most sold product |
---
## ⚠️ Data Modeling Considerations

- The dataset combines:
  - Order-level data (`orders`)
  - Item-level data (`order_items`)

- After joining, order-level metrics (e.g. `total_amount`) may be duplicated across multiple rows.

- This pipeline assumes:
  - Aggregations are handled carefully to avoid double counting

- In production scenarios:
  - It is recommended to compute metrics at the correct grain (e.g. line-level or order-level separately)

## 🧠 Key Concepts Demonstrated

- Data Cleaning with Pandas  
- Handling Missing Data  
- Deduplication Strategies  
- GroupBy + Aggregations  
- Joins (`merge`)  
- Time-based analysis  
- Export to Parquet (data engineering best practice)  

---

## 🧪 Data Validation

The pipeline includes:

- File existence checks  
- Data exploration (`head()`, `info()`)  
- Null analysis  
- Duplicate detection  

---

## 📌 Future Improvements

- Add logging instead of prints  
- Parameterize file paths  
- Add unit tests  
- Convert to modular pipeline (functions)  
- Orchestrate with Airflow  
- Load into a Data Warehouse (BigQuery / Snowflake)  

---

## 💡 Author

Mauricio Lancheros  
Data Engineer | Python | SQL | ETL Pipelines