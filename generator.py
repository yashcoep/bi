import csv
import random
import faker
from datetime import timedelta
import datetime

fake = faker.Faker()

# Define products with IDs
products = [
    (1, "Grilled Salmon", "Fish", 15.99),
    (2, "Fish and Chips", "Fish", 12.99),
    (3, "Tuna Steak", "Fish", 18.99),
    (4, "Lobster Roll", "Fish", 22.99),
    (5, "Fried Calamari", "Fish", 11.99),
    (6, "Sea Bass", "Fish", 17.99),
    (7, "Mussels Marinara", "Fish", 14.99),
    (8, "Grilled Shrimp", "Fish", 16.49),
    (9, "Smoked Trout", "Fish", 19.99),
    (10, "Sushi Platter", "Fish", 24.99),
    (11, "Crab Cakes", "Fish", 20.99),
    (12, "Cod Fillet", "Fish", 13.99),
    (13, "Oysters", "Fish", 21.49),
    (14, "Mahi Mahi", "Fish", 17.99),
    (15, "Swordfish Steak", "Fish", 23.99),
    (16, "Chocolate Lava Cake", "Dessert", 7.99),
    (17, "Apple Pie", "Dessert", 5.99),
    (18, "Cheesecake", "Dessert", 6.99),
    (19, "Red Berry Compote", "Dessert", 5.49),
    (20, "Vanilla Quark", "Dessert", 4.99)
]

# Seasonal Mapping
seasonal_mapping = {
    "Winter": ["Grilled Salmon", "Fish and Chips", "Tuna Steak", "Lobster Roll"],
    "Spring": ["Fried Calamari", "Sea Bass", "Mussels Marinara", "Grilled Shrimp"],
    "Summer": ["Smoked Trout", "Sushi Platter", "Crab Cakes", "Cod Fillet"],
    "Autumn": ["Oysters", "Mahi Mahi", "Swordfish Steak"]
}

# Generate Stores
stores = [{"store_id": i, "name": fake.company(), "city": fake.city()} for i in range(1, 401)]

# Generate Customers
customers = [{"customer_id": i, "name": fake.name(), "zip_code": fake.zipcode()} for i in range(1, 1001)]

def get_season(month):
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Autumn"

def random_datetime_2024():
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 12, 31, 23, 59, 59)
    random_sec = random.randint(0, int((end - start).total_seconds()))
    return start + timedelta(seconds=random_sec)

# Generate Orders
sales_orders = []
order_details = []
invoices = []
payments = []
inventory = {}

for order_id in range(1, 5001):
    store = random.choice(stores)
    customer = random.choice(customers)
    order_date = random_datetime_2024()
    season = get_season(order_date.month)
    total_amount = 0

    num_products = random.randint(1, 7)

    # Prepare weights
    weights = []
    for p in products:
        if p[2] == "Fish":
            if p[1] in seasonal_mapping[season]:
                weights.append(0.7)
            else:
                weights.append(0.3)
        else:
            weights.append(1.0)

    selected_products = random.choices(products, weights=weights, k=num_products)

    for product in selected_products:
        product_id = product[0]
        quantity = random.randint(1, 3)
        unit_price = product[3]
        total_price = quantity * unit_price
        total_amount += total_price
        order_details.append({"order_id": order_id, "product_id": product_id, "quantity": quantity, "unit_price": unit_price, "total_price": total_price})

        # Update inventory
        inventory_key = (store["store_id"], product_id)
        inventory[inventory_key] = inventory.get(inventory_key, 50) - quantity

    # Invoice
    invoice_id = order_id
    invoice_date = order_date.strftime("%d-%m-%Y")
    invoices.append({"invoice_id": invoice_id, "order_id": order_id, "invoice_date": invoice_date, "total_invoice_amount": total_amount})

    # Payment
    payment_id = order_id
    payment_method = random.choices(["Credit Card", "Cash", "Mobile Payment"], weights=[60, 30, 10])[0]
    payment_status = random.choices(["Completed", "Declined", "Timeout"], weights=[80, 15, 5])[0]
    payment_date = order_date.strftime("%m/%d/%Y")
    payments.append({"payment_id": payment_id, "payment_method": payment_method, "amount": total_amount, "status": payment_status, "payment_date": payment_date})

    # Sales Order
    sales_orders.append({"order_id": order_id, "store_id": store["store_id"], "customer_id": customer["customer_id"], "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"), "total_amount": total_amount})

# Inventory Data
inventory_data = [{"store_id": key[0], "product_id": key[1], "stock_quantity": max(0, stock), "last_updated": fake.date_this_year()} for key, stock in inventory.items()]

# Write CSV
def write_csv(filename, fieldnames, data):
    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

write_csv("stores.csv", ["store_id", "name", "city"], stores)
write_csv("products.csv", ["product_id", "name", "category", "price"], [{"product_id": p[0], "name": p[1], "category": p[2], "price": p[3]} for p in products])
write_csv("customers.csv", ["customer_id", "name", "zip_code"], customers)
write_csv("sales_orders.csv", ["order_id", "store_id", "customer_id", "order_date", "total_amount"], sales_orders)
write_csv("order_details.csv", ["order_id", "product_id", "quantity", "unit_price", "total_price"], order_details)
write_csv("invoices.csv", ["invoice_id", "order_id", "invoice_date", "total_invoice_amount"], invoices)
write_csv("payments.csv", ["payment_id", "payment_method", "amount", "status", "payment_date"], payments)
write_csv("inventory.csv", ["store_id", "product_id", "stock_quantity", "last_updated"], inventory_data)

print("✅ Data generation complete with correct seasonal impact & product IDs.")
