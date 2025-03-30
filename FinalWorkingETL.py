import petl as etl
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import oracledb

# ---------------------- 1. Extract & Store Raw Data ----------------------
raw_data = {
    "customers": etl.fromcsv("customers.csv"),
    "products": etl.fromcsv("products.csv"),
    "stores": etl.fromcsv("stores.csv"),
    "inventory": etl.fromcsv("inventory.csv"),
    "payments": etl.fromcsv("payments.csv"),
    "invoices": etl.fromcsv("invoices.csv"),
    "order_details": etl.fromcsv("order_details.csv"),
    "sales_orders": etl.fromcsv("sales_orders.csv")
}

sales_orders_copy = etl.wrap(raw_data["sales_orders"])  # Safe copy

# ---------------------- 2. Standardize Dates ----------------------
def standardize_date(value):
    if not value or value.strip() == "":
        return None
    formats = ["%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value

transformed_data = {
    "sales_orders": etl.convert(raw_data["sales_orders"], "order_date", standardize_date),
    "payments": etl.convert(raw_data["payments"], "payment_date", standardize_date),
    "invoices": etl.convert(raw_data["invoices"], "invoice_date", standardize_date),
    "inventory": etl.convert(raw_data["invoices"], "last_updated", standardize_date)

}


def get_season(date_str):
    month = int(date_str.split("-")[1])  # Extract month
    if month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Fall"
    else:
        return "Winter"
# ---------------------- Categorize Orders by Weekday vs. Weekend ----------------------
def get_weekday_category(date_str):
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    weekday = date_obj.weekday()
    return "Weekend" if weekday >= 5 else "Weekday"

enriched_data = {}

enriched_data["fact_sales"] = etl.join(sales_orders_copy, raw_data["order_details"], key="order_id")
orders_arg = etl.join(transformed_data["payments"], transformed_data["invoices"], lkey="payment_id", rkey="invoice_id")
arg_stores = etl.join(orders_arg, sales_orders_copy, key="order_id")
enriched_data["fact_sales"] = etl.leftjoin(enriched_data["fact_sales"], arg_stores, key="order_id")
enriched_data["fact_sales"] = etl.addfield(enriched_data["fact_sales"], "season", lambda row: get_season(row["order_date"]))
enriched_data["fact_sales"] = etl.addfield(enriched_data["fact_sales"], "day_category", lambda row: get_weekday_category(row["order_date"]))

import os

print("enriched_data")
print(enriched_data)

# Create output directory if it doesn't exist
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# Export enriched data to CSV files inside the output folder
for table_name, table_data in enriched_data.items():
    file_path = os.path.join(output_dir, f"{table_name}.csv")
    etl.tocsv(table_data, file_path)


print(f"Export complete. Check the CSV files in the '{output_dir}' folder.")

dsn = "localhost:1521/mars_local"
user = "bi_sales"
password = "bi_sales_password"

try:
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    print("✅ Successfully connected to Oracle Database!")
    cursor = conn.cursor()
except oracledb.DatabaseError as e:
    print("❌ Error:", e)
    exit()


def to_oracle_date(value):
    """Convert string dates to datetime objects or return None."""
    if isinstance(value, datetime.datetime):  # Already a datetime object
        return value
    if not value or value.strip() == "":
        return None
    formats = ["%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    return None  # If all formats fail, return None

def load_data(table_name, data, columns):
    """Insert data into Oracle tables."""
    placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])  
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for row in etl.dicts(data):  # Convert rows to dictionary format
        processed_row = []
        for col in columns:
            value = row.get(col)
            print(value)
            if "DATE" in col.upper():  # Check if column is a date column
                value = to_oracle_date(value)  # Convert to datetime
            processed_row.append(value)

        try:
            cursor.execute(sql, tuple(processed_row))
        except oracledb.DatabaseError as e:
            print(f"❌ Error inserting into {table_name}: {e}")
            break

    conn.commit()
    print(f"✅ Loaded {etl.nrows(data)} records into {table_name}")


print(raw_data["customers"])
# Usage example
load_data("DT_Customers", raw_data["customers"], ["customer_id", "name",  "zip_code"])
load_data("DT_Products", raw_data["products"], ["product_id", "name", "category", "price"])
load_data("DT_Stores", raw_data["stores"], ["store_id", "city", "name"])
load_data("DT_Payments", transformed_data["payments"], ["payment_id", "payment_method", "status", "payment_date"])
load_data("DT_Invoices", transformed_data["invoices"], ["invoice_id", "totat_invoice_amount", "order_id", "invoice_date"])
load_data("FT_Sales", enriched_data["fact_sales"], ["order_id", "customer_id", "store_id", "product_id", "order_date", "season", "day_category", "quantity", "unit_price", "total_price", "payment_id", "invoice_id"])

