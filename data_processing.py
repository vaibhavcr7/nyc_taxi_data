import pandas as pd
import os

DATA_DIR = "data/2019/"
PROCESSED_DIR = "data/processed/"
os.makedirs(PROCESSED_DIR, exist_ok=True)


def convert_parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file)
    df.to_csv(csv_file, index=False)
    print(f"Converted {parquet_file} to {csv_file}")


def process_file(month):
    # Define file paths
    parquet_file_path = os.path.join(DATA_DIR, f"yellow_tripdata_2019-{month:02d}.parquet")
    csv_file_path = os.path.join(DATA_DIR, f"yellow_tripdata_2019-{month:02d}.csv")

    # Convert Parquet to CSV if Parquet file exists
    if os.path.exists(parquet_file_path):
        convert_parquet_to_csv(parquet_file_path, csv_file_path)
    else:
        print(f"No Parquet file found for month {month}, skipping conversion.")
        return

    # Process the CSV file
    try:
        df = pd.read_csv(csv_file_path, low_memory=False)
    except Exception as e:
        print(f"Error reading CSV file {csv_file_path}: {e}")
        return

    # Remove rows with missing or corrupt data
    df.dropna(inplace=True)

    # Derive new columns
    df['pickup_time'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['dropoff_time'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['trip_duration'] = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)

    # Aggregate data
    daily_summary = df.groupby(df['pickup_time'].dt.date).agg(
        total_trips=('VendorID', 'count'),
        average_fare=('fare_amount', 'mean')
    ).reset_index()

    # Save processed data
    processed_file_path = os.path.join(PROCESSED_DIR, f"yellow_tripdata_2019-{month:02d}_processed.csv")
    daily_summary.to_csv(processed_file_path, index=False)
    print(f"Processed month {month}")


if __name__ == "__main__":
    for month in range(1, 13):
        process_file(month)
