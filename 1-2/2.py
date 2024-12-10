import sqlite3
import pandas as pd
import json

pkl_file_path = "item.pkl"
json_file_path = "subitem.json"

VAR = 25
LIMIT = VAR + 10

books_data = pd.read_pickle(pkl_file_path)
books_df = pd.DataFrame(books_data)

with open(json_file_path, "r", encoding="utf-8") as f:
    sales_data = json.load(f)
sales_df = pd.DataFrame(sales_data)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        author TEXT,
        genre TEXT,
        pages INTEGER,
        published_year INTEGER,
        isbn TEXT,
        rating REAL,
        views INTEGER
    )
"""
)

insert_books_query = """
    INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
for _, row in books_df.iterrows():
    cursor.execute(
        insert_books_query,
        (
            row["title"],
            row["author"],
            row["genre"],
            int(row["pages"]),
            int(row["published_year"]),
            row["isbn"],
            float(row["rating"]),
            int(row["views"]),
        ),
    )
conn.commit()

cursor.execute(
    """
    CREATE TABLE sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        price REAL,
        place TEXT,
        date TEXT,
        FOREIGN KEY (title) REFERENCES books(title)
    )
"""
)

insert_sales_query = """
    INSERT INTO sales (title, price, place, date)
    VALUES (?, ?, ?, ?)
"""
for _, row in sales_df.iterrows():
    cursor.execute(
        insert_sales_query,
        (row["title"], float(row["price"]), row["place"], row["date"]),
    )
conn.commit()

query1 = """
    SELECT b.genre, AVG(s.price) AS avg_price
    FROM books b
    JOIN sales s ON b.title = s.title
    GROUP BY b.genre
    ORDER BY avg_price DESC
"""
cursor.execute(query1)
avg_price_by_genre = cursor.fetchall()

query2 = """
    SELECT b.genre, s.place, COUNT(*) AS sales_count
    FROM books b
    JOIN sales s ON b.title = s.title
    GROUP BY b.genre, s.place
    ORDER BY b.genre, sales_count DESC
"""
cursor.execute(query2)
sales_count_by_genre_and_place = cursor.fetchall()

query3 = """
    SELECT b.title, b.views, COUNT(s.id) AS sales_count
    FROM books b
    LEFT JOIN sales s ON b.title = s.title
    GROUP BY b.title, b.views
    ORDER BY b.views DESC
    LIMIT 5
"""
cursor.execute(query3)
top_books_by_views = cursor.fetchall()

query4 = """
    SELECT b.author, SUM(s.price) AS total_sales
    FROM books b
    JOIN sales s ON b.title = s.title
    GROUP BY b.author
    ORDER BY total_sales DESC
"""
cursor.execute(query4)
total_sales_by_author = cursor.fetchall()

output_files = {
    "avg_price_by_genre_2.json": [
        {"genre": row[0], "avg_price": row[1]} for row in avg_price_by_genre
    ],
    "sales_count_by_genre_and_place_2.json": [
        {"genre": row[0], "place": row[1], "sales_count": row[2]}
        for row in sales_count_by_genre_and_place
    ],
    "top_books_by_views_2.json": [
        {"title": row[0], "views": row[1], "sales_count": row[2]}
        for row in top_books_by_views
    ],
    "total_sales_by_author_2.json": [
        {"author": row[0], "total_sales": row[1]} for row in total_sales_by_author
    ],
}

for filename, data in output_files.items():
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

conn.close()

print("Результаты сохранены в файлы:")
for filename in output_files:
    print(filename)
