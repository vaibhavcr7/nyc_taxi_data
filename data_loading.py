import pandas as pd
import sqlite3
import os

PROCESSED_DIR = "data/processed/"
DATABASE = "nyc_taxi_data.db"

conn = sqlite3.connect(DATABASE)
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_summary (
    date TEXT PRIMARY KEY,
    total_trips INTEGER,
    average_fare REAL
)
""")


def load_data(month):
    processed_file_path = os.path.join(PROCESSED_DIR, f"yellow_tripdata_2019-{month:02d}_processed.csv")
    df = pd.read_csv(processed_file_path)
    df.to_sql('daily_summary', conn, if_exists='append', index=False)


if __name__ == "__main__":
    for month in range(1, 13):
        load_data(month)
        print(f"Loaded data for month {month}")

conn.close()
