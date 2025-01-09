import sqlite3
import pandas as pd
import json


LIMIT = 25 + 10

pkl_file_path = "item.pkl"

books_data = pd.read_pickle(pkl_file_path)

books_df = pd.DataFrame(books_data)

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

insert_query = """
    INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
for _, row in books_df.iterrows():
    cursor.execute(
        insert_query,
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

query1 = f"""
    SELECT * FROM books
    ORDER BY views DESC
    LIMIT {LIMIT}
"""
cursor.execute(query1)
sorted_books = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]
sorted_books_json = [dict(zip(columns, row)) for row in sorted_books]

query2 = """
    SELECT SUM(rating) AS sum_rating,
           MIN(rating) AS min_rating,
           MAX(rating) AS max_rating,
           AVG(rating) AS avg_rating
    FROM books
"""
cursor.execute(query2)
rating_stats = cursor.fetchone()

query3 = """
    SELECT genre, COUNT(*) AS count
    FROM books
    GROUP BY genre
    ORDER BY count DESC
"""
cursor.execute(query3)
genre_frequency = cursor.fetchall()

query4 = f"""
    SELECT * FROM books
    WHERE pages > 200
    ORDER BY rating DESC
    LIMIT {LIMIT}
"""
cursor.execute(query4)
filtered_books = cursor.fetchall()

filtered_books_json = [dict(zip(columns, row)) for row in filtered_books]

output_files = {
    "sorted_books.json": sorted_books_json,
    "rating_stats.json": {
        "sum_rating": rating_stats[0],
        "min_rating": rating_stats[1],
        "max_rating": rating_stats[2],
        "avg_rating": rating_stats[3],
    },
    "genre_frequency.json": [
        {"genre": row[0], "count": row[1]} for row in genre_frequency
    ],
    "filtered_books.json": filtered_books_json,
}

for filename, data in output_files.items():
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

conn.close()

for filename in output_files:
    print(filename)
