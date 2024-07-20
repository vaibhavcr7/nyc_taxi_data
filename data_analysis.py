import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DATABASE = "nyc_taxi_data.db"

conn = sqlite3.connect(DATABASE)


def query_data(query):
    return pd.read_sql_query(query, conn)


peak_hours_query = """
SELECT strftime('%H', tpep_pickup_datetime) AS hour, COUNT(*) AS trips
FROM trips
GROUP BY hour
ORDER BY trips DESC
"""
peak_hours = query_data(peak_hours_query)

# Visualization: Peak hours for taxi usage
plt.figure(figsize=(10, 6))
plt.bar(peak_hours['hour'], peak_hours['trips'])
plt.xlabel('Hour of Day')
plt.ylabel('Number of Trips')
plt.title('Peak Hours for Taxi Usage')
plt.xticks(rotation=45)
plt.show()
