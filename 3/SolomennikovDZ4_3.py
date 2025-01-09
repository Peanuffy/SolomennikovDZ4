import sqlite3
import pandas as pd
import msgpack
import json

text_file_path = "_part_1.text"
msgpack_file_path = "_part_2.msgpack"

VAR = 25
LIMIT = VAR + 10


def parse_text_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read().strip().split("=====")

    parsed_data = []
    for entry in data:
        song_info = {}
        for line in entry.strip().split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                song_info[key.strip()] = value.strip()
        if song_info:
            parsed_data.append(song_info)

    return pd.DataFrame(parsed_data)


songs_df_text = parse_text_file(text_file_path)


def parse_msgpack_file(file_path):
    with open(file_path, "rb") as f:
        data = msgpack.unpack(f)

    parsed_data = []
    for entry in data:
        song_info = {}
        for key in entry:
            song_info[key] = entry[key]
        parsed_data.append(song_info)

    return pd.DataFrame(parsed_data)


songs_df_msgpack = parse_msgpack_file(msgpack_file_path)

if "explicit" not in songs_df_msgpack.columns:
    songs_df_msgpack["explicit"] = False

if "loudness" not in songs_df_msgpack.columns:
    songs_df_msgpack["loudness"] = 0.0


conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        instrumentalness REAL,
        explicit BOOLEAN,
        loudness REAL
    )
"""
)

insert_song_query = """
    INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in songs_df_text.iterrows():
    cursor.execute(
        insert_song_query,
        (
            row["artist"],
            row["song"],
            int(row["duration_ms"]),
            int(row["year"]),
            float(row["tempo"]),
            row["genre"],
            float(row["instrumentalness"]),
            row["explicit"] == "True",
            float(row["loudness"]),
        ),
    )

for _, row in songs_df_msgpack.iterrows():
    cursor.execute(
        insert_song_query,
        (
            row["artist"],
            row["song"],
            int(row["duration_ms"]),
            int(row["year"]),
            float(row["tempo"]),
            row["genre"],
            float(row["instrumentalness"]),
            row["explicit"] == 1,
            float(row["loudness"]),
        ),
    )

conn.commit()

query1 = f"""
    SELECT * FROM songs
    ORDER BY duration_ms DESC
    LIMIT {LIMIT}
"""
cursor.execute(query1)
sorted_songs = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]
sorted_songs_json = [dict(zip(columns, row)) for row in sorted_songs]

query2 = """
    SELECT genre,
           SUM(duration_ms) AS sum_duration,
           MIN(duration_ms) AS min_duration,
           MAX(duration_ms) AS max_duration,
           AVG(duration_ms) AS avg_duration
    FROM songs
    GROUP BY genre
"""
cursor.execute(query2)
duration_analysis = cursor.fetchall()

query3 = """
    SELECT genre, COUNT(*) AS count
    FROM songs
    GROUP BY genre
    ORDER BY count DESC
"""
cursor.execute(query3)
genre_frequency = cursor.fetchall()

query4 = """
    SELECT artist, song, tempo, genre
    FROM songs
    WHERE tempo > 120
    ORDER BY tempo DESC
"""
cursor.execute(query4)
filtered_songs = cursor.fetchall()

output_files = {
    "sorted_songs.json": sorted_songs_json,
    "duration_analysis.json": [
        {
            "genre": row[0],
            "sum_duration": row[1],
            "min_duration": row[2],
            "max_duration": row[3],
            "avg_duration": row[4],
        }
        for row in duration_analysis
    ],
    "genre_frequency.json": [
        {"genre": row[0], "count": row[1]} for row in genre_frequency
    ],
    "filtered_songs.json": [
        {"artist": row[0], "song": row[1], "tempo": row[2], "genre": row[3]}
        for row in filtered_songs
    ],
}

for filename, data in output_files.items():
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

conn.close()

