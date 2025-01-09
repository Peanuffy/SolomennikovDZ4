import sqlite3
import pandas as pd
import msgpack
import json

text_file_path = "_product_data.text"
msgpack_file_path = "_update_data.msgpack"

VAR = 25
LIMIT = VAR + 10


def parse_text_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read().strip().split("=====")

    parsed_data = []
    for entry in data:
        product_info = {}
        for line in entry.strip().split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                product_info[key.strip()] = value.strip()
        if product_info:
            parsed_data.append(product_info)

    return pd.DataFrame(parsed_data)


products_df_text = parse_text_file(text_file_path)


def parse_msgpack_file(file_path):
    with open(file_path, "rb") as f:
        data = msgpack.unpack(f)

    parsed_data = []
    for entry in data:
        product_info = {}
        for key in entry:
            product_info[key] = entry[key]
        parsed_data.append(product_info)

    return pd.DataFrame(parsed_data)


products_df_msgpack = parse_msgpack_file(msgpack_file_path)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        quantity INTEGER,
        category TEXT,
        fromCity TEXT,
        isAvailable BOOLEAN,
        views INTEGER,
        update_counter INTEGER DEFAULT 0
    )
"""
)

insert_product_query = """
    INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

for _, row in products_df_text.iterrows():
    price = max(float(row["price"]), 0)
    cursor.execute(
        insert_product_query,
        (
            row["name"],
            price,
            int(row["quantity"]),
            row.get("category", None),
            row["fromCity"],
            row["isAvailable"] == "True",
            int(row["views"]),
        ),
    )
conn.commit()

for _, row in products_df_msgpack.iterrows():
    name = row.get("name")
    method = row.get("method")
    param = row.get("param")

    if method == "price_abs":
        new_price = max(float(param), 0)
        cursor.execute(
            """
            UPDATE products
            SET price = ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (new_price, name),
        )

    elif method == "price_percent":
        percent = float(param)
        cursor.execute(
            """
            UPDATE products
            SET price = MAX(price * (1 + ? / 100), 0), update_counter = update_counter + 1
            WHERE name = ?
        """,
            (percent, name),
        )

    elif method == "quantity_add":
        quantity_add = int(param)
        cursor.execute(
            """
            UPDATE products
            SET quantity = quantity + ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (quantity_add, name),
        )

    elif method == "quantity_sub":
        quantity_sub = int(param)
        cursor.execute(
            """
            UPDATE products
            SET quantity = quantity - ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (quantity_sub, name),
        )

    elif method == "available":
        available = param == "True"
        cursor.execute(
            """
            UPDATE products
            SET isAvailable = ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (available, name),
        )

    elif method == "remove":
        cursor.execute(
            """
            DELETE FROM products
            WHERE name = ?
        """,
            (name,),
        )
conn.commit()

cursor.execute("SELECT * FROM products")
all_products = cursor.fetchall()
print("Текущие данные о товарах:")
for product in all_products:
    print(product)

query1 = """
    SELECT name, update_counter
    FROM products
    ORDER BY update_counter DESC
    LIMIT 10
"""
cursor.execute(query1)
top_updated_products = cursor.fetchall()

query2 = """
    SELECT category,
           SUM(price) AS sum_price,
           MIN(price) AS min_price,
           MAX(price) AS max_price,
           AVG(price) AS avg_price,
           COUNT(*) AS count
    FROM products
    GROUP BY category
"""
cursor.execute(query2)
price_analysis = cursor.fetchall()

query3 = """
    SELECT category,
           SUM(quantity) AS sum_quantity,
           MIN(quantity) AS min_quantity,
           MAX(quantity) AS max_quantity,
           AVG(quantity) AS avg_quantity
    FROM products
    GROUP BY category
"""
cursor.execute(query3)
quantity_analysis = cursor.fetchall()

query4 = """
    SELECT name, price, quantity, category
    FROM products
    WHERE price < 100
"""
cursor.execute(query4)
expensive_products = cursor.fetchall()

output_files = {
    "top_updated_products.json": top_updated_products,
    "price_analysis.json": [
        {
            "category": row[0],
            "sum_price": row[1],
            "min_price": row[2],
            "max_price": row[3],
            "avg_price": row[4],
            "count": row[5],
        }
        for row in price_analysis
    ],
    "quantity_analysis.json": [
        {
            "category": row[0],
            "sum_quantity": row[1],
            "min_quantity": row[2],
            "max_quantity": row[3],
            "avg_quantity": row[4],
        }
        for row in quantity_analysis
    ],
    "expensive_products.json": expensive_products,
}

for filename, data in output_files.items():
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

conn.close()

print("Результаты сохранены в файлы:")
for filename in output_files:
    print(filename)
