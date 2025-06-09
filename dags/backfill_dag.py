from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import os
import time
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, col, lit, hour, avg, trim
from elasticsearch import Elasticsearch
import json
from pyspark.sql.types import IntegerType
from collections import Counter

# Configuration
CITIES_CSV = '/home/ccd/airflow/config/european_cities.csv'
RAW_POLLUTION_DIR = '/home/ccd/airflow/data/raw/pollution'
RAW_WEATHER_DIR = '/home/ccd/airflow/data/raw/weather'
FORMATTED_POLLUTION_DIR = '/home/ccd/airflow/data/formatted/pollution'
FORMATTED_WEATHER_DIR = '/home/ccd/airflow/data/formatted/weather'
START_DATE = '2025-05-20'
END_DATE = '2025-05-27'
POLLUTION_API_KEY = '7815b1bfdf9c82631772b3e93f87f69c'
FORMATTED_COMBINED_DIR = '/home/ccd/airflow/data/formatted/combined'


def fetch_air_pollution(city):
    lat = city['latitude']
    lon = city['longitude']
    city_name = city['name'].lower().replace(" ", "_")

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    start_unix = int(start_dt.timestamp())
    end_unix = int(end_dt.timestamp())

    url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
    params = {
        'lat': lat,
        'lon': lon,
        'start': start_unix,
        'end': end_unix,
        'appid': POLLUTION_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    folder = os.path.join(RAW_POLLUTION_DIR, city_name)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"full_{START_DATE}_{END_DATE}.json")

    with open(filename, 'w') as f:
        json.dump(data, f)
# Fetch historical weather data
def fetch_weather(city):
    lat = city['latitude']
    lon = city['longitude']
    city_name = city['name'].lower().replace(" ", "_")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': START_DATE,
        'end_date': END_DATE,
        'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,snowfall_sum,cloudcover_mean,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration',
        'timezone': 'Europe/Paris'
    }

    response = requests.get(url, params=params)
    data = response.json()

    folder = os.path.join(RAW_WEATHER_DIR, city_name)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"full_{START_DATE}_{END_DATE}.json")

    with open(filename, 'w') as f:
        json.dump(data, f)

# Fetch all cities
def fetch_all_data_backfill():
    cities = load_cities()
    for city in cities:
        fetch_air_pollution(city)
        time.sleep(2)
        fetch_weather(city)
        time.sleep(2)

# Load cities
def load_cities():
    return pd.read_csv(CITIES_CSV).to_dict(orient='records')

# Flatten pollution
def flatten_pollution(json_data):
    flattened = []
    lon = json_data.get('coord', {}).get('lon')
    lat = json_data.get('coord', {}).get('lat')
    for item in json_data.get('list', []):
        row = {
            'lon': lon,
            'lat': lat,
            'dt': item.get('dt'),
            'aqi': item.get('main', {}).get('aqi')
        }
        row.update(item.get('components', {}))
        flattened.append(row)
    df = pd.DataFrame(flattened)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['dt'], unit='s', errors='coerce', utc=True)
    return df

# Flatten weather
def flatten_weather(json_data):
    daily_data = json_data.get('daily', {})
    records = []
    num_days = len(daily_data.get('time', []))
    for i in range(num_days):
        record = {}
        for key, values in daily_data.items():
            try:
                val = values[i]
                record[key] = str(val) if isinstance(val, (list, dict)) else (None if pd.isna(val) else val)
            except:
                record[key] = None
        record['lat'] = json_data.get('latitude')
        record['lon'] = json_data.get('longitude')
        records.append(record)
    df = pd.DataFrame(records).fillna('')
    return df

# Aggregate pollution by day
def aggregate_pollution(df: pd.DataFrame) -> pd.DataFrame:
    df['hour'] = df['timestamp'].dt.hour
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df_daytime = df[(df['hour'] >= 6) & (df['hour'] <= 23)]
    if df_daytime.empty:
        return pd.DataFrame()
    components = ['co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3']
    avg_components = df_daytime[components].mean().round(3).to_dict()
    aqi_mode = df_daytime['aqi'].mode().iloc[0] if not df_daytime['aqi'].mode().empty else None
    result = {
        **avg_components,
        'aqi_mode': aqi_mode,
        'lat': df_daytime['lat'].iloc[0],
        'lon': df_daytime['lon'].iloc[0],
        'time': df_daytime['date'].iloc[2]
    }
    return pd.DataFrame([result])

# Format backfill data into daily folders
def format_backfill_all_dates():
    spark = SparkSession.builder.appName("BackfillFormat").master("local[*]").getOrCreate()
    cities = load_cities()
    for city in cities:
        city_name = city['name'].lower().replace(" ", "_")
        print(f"[CITY] Processing: {city_name}")

        for path_type, raw_dir, formatted_dir, flatten_func in [
            ("pollution", RAW_POLLUTION_DIR, FORMATTED_POLLUTION_DIR, flatten_pollution),
            ("weather", RAW_WEATHER_DIR, FORMATTED_WEATHER_DIR, flatten_weather)
        ]:
            raw_city_dir = os.path.join(raw_dir, city_name)
            formatted_city_dir = os.path.join(formatted_dir, city_name)
            os.makedirs(formatted_city_dir, exist_ok=True)

            for filename in os.listdir(raw_city_dir):
                if not filename.endswith('.json'):
                    continue
                raw_path = os.path.join(raw_city_dir, filename)
                with open(raw_path, 'r') as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        print(f"[SKIP] Corrupted file: {raw_path}")
                        continue



                if path_type == "pollution":
                    df = flatten_func(data)

                    if df.empty:
                        continue
                    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                    for date in df['date'].unique():
                        df_day = df[df['date'] == date]
                        df_agg = aggregate_pollution(df_day)
                        if df_agg.empty:
                            continue
                        df_spark = spark.createDataFrame(df_agg).withColumn("city_name", lit(city_name))
                        output_path = os.path.join(formatted_city_dir, date)  # No '.parquet'
                        df_spark.write.mode("overwrite").parquet(output_path)
                        print(f"[OK] Pollution written: {output_path}")
                else:
                    df = flatten_func(data)
                    if df.empty or "time" not in df.columns:
                        continue
                    for date in df["time"].unique():
                        df_day = df[df["time"] == date]
                        df_spark = spark.createDataFrame(df_day).withColumn("city_name", lit(city_name))
                        output_path = os.path.join(formatted_city_dir, date)  # No '.parquet'
                        df_spark.write.mode("overwrite").parquet(output_path)
                        print(f"[OK] Weather written: {output_path}")
    spark.stop()


def combine_backfill_data():
    spark = SparkSession.builder.appName("CombineBackfillData").master("local[*]").getOrCreate()
    cities = load_cities()

    for city in cities:
        city_name = city['name'].lower().replace(" ", "_")
        print(f"[COMBINING] City: {city_name}")

        pollution_dir = os.path.join(FORMATTED_POLLUTION_DIR, city_name)
        weather_dir = os.path.join(FORMATTED_WEATHER_DIR, city_name)
        combined_dir = os.path.join(FORMATTED_COMBINED_DIR, city_name)
        os.makedirs(combined_dir, exist_ok=True)

        pollution_dates = set(os.listdir(pollution_dir)) if os.path.exists(pollution_dir) else set()
        weather_dates = set(os.listdir(weather_dir)) if os.path.exists(weather_dir) else set()
        common_dates = sorted(pollution_dates & weather_dates)

        for date in common_dates:
            pollution_path = os.path.join(pollution_dir, date)
            weather_path = os.path.join(weather_dir, date)
            output_path = os.path.join(combined_dir, date)

            try:
                df_poll = spark.read.parquet(pollution_path).withColumn("city_name", trim(col("city_name")))
                df_weather = spark.read.parquet(weather_path).withColumn("city_name", trim(col("city_name")))
                # Drop conflicting columns from weather
                df_weather = df_weather.drop("lat", "lon")

                # Join only on city_name and time
                df_combined = df_poll.join(
                    df_weather,
                    on=["city_name", "time"],
                    how="inner"
                )

                if not df_combined.rdd.isEmpty():
                    df_combined.write.mode("overwrite").parquet(output_path)
                    print(f"[OK] Combined {city_name} {date}")
                else:
                    print(f"[SKIP] No rows after join for {city_name} {date}")
            except Exception as e:
                print(f"[ERROR] Failed to combine {city_name} {date}: {e}")

    spark.stop()
    print("[DONE] Backfill combining completed")
def index_backfill_to_elasticsearch():
    spark = SparkSession.builder.appName("IndexBackfillES").master("local[*]").getOrCreate()
    # es = Elasticsearch("http://localhost:9200")
    es = Elasticsearch("http://elasticsearch:9200")
    index_name = "environment_data"

    try:
        es.info()
    except Exception as e:
        print(f"[ERROR] Cannot connect to Elasticsearch: {e}")
        return

    for city in os.listdir(FORMATTED_COMBINED_DIR):
        city_path = os.path.join(FORMATTED_COMBINED_DIR, city)
        if not os.path.isdir(city_path):
            continue

        for date in os.listdir(city_path):
            parquet_path = os.path.join(city_path, date)

            try:
                df = spark.read.parquet(parquet_path)
                records = df.toJSON().collect()

                for record in records:
                    doc = json.loads(record)
                    es.index(index=index_name, document=doc)

                print(f"[OK] Indexed {city}/{date}")
            except Exception as e:
                print(f"[ERROR] Failed to index {city}/{date}: {e}")

    spark.stop()
    print("[DONE] Finished indexing backfill data to Elasticsearch")
# -*
# Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='backfill_pollution_weather_dag',
    default_args=default_args,
    description='One-time backfill of air pollution and weather data (split and formatted like daily DAG)',
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['pollution', 'weather', 'backfill'],
) as dag:

    format_all_backfill = PythonOperator(
        task_id='format_backfill_all_data',
        python_callable=format_backfill_all_dates
    )

    fetch_task = PythonOperator(
        task_id='fetch_full_history_all_cities',
        python_callable=fetch_all_data_backfill
    )

    combine_backfill = PythonOperator(
        task_id='combine_backfill_data',
        python_callable=combine_backfill_data
    )
    index_backfill = PythonOperator(
        task_id='index_backfill_combined_to_elasticsearch',
        python_callable=index_backfill_to_elasticsearch
    )

    fetch_task >> format_all_backfill >> combine_backfill >> index_backfill