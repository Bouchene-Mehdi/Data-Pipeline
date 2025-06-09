from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import os
import time
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import trim, col
from elasticsearch import Elasticsearch

# Configuration
CITIES_CSV = '/home/ccd/airflow/config/european_cities.csv'
RAW_POLLUTION_DIR = '/home/ccd/airflow/data/raw/pollution'
RAW_WEATHER_DIR = '/home/ccd/airflow/data/raw/weather'
FORMATTED_POLLUTION_DIR = '/home/ccd/airflow/data/formatted/pollution'
FORMATTED_WEATHER_DIR = '/home/ccd/airflow/data/formatted/weather'
POLLUTION_API_KEY = '7815b1bfdf9c82631772b3e93f87f69c'
FORMATTED_COMBINED_DIR = '/home/ccd/airflow/data/formatted/combined'

# Load cities from CSV
def load_cities():
    return pd.read_csv(CITIES_CSV).to_dict(orient='records')

# Fetch air pollution data
def fetch_air_pollution(city, execution_date):
    lat = city['latitude']
    lon = city['longitude']
    city_name = city['name'].lower().replace(" ", "_")

    start_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    now = datetime.utcnow()
    if end_dt > now:
        end_dt = now

    start_unix = int(start_dt.timestamp())
    end_unix = int(end_dt.timestamp())

    folder = os.path.join(RAW_POLLUTION_DIR, city_name)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{execution_date}.json")

    if os.path.exists(filename) and os.path.getsize(filename) > 100:
        print(f"[SKIP] Pollution data already exists: {filename}")
        return

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
    with open(filename, 'w') as f:
        json.dump(data, f)

# Fetch weather data
def fetch_weather(city, execution_date):
    lat = city['latitude']
    lon = city['longitude']
    city_name = city['name'].lower().replace(" ", "_")

    start_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    end_dt = start_dt
    now = datetime.utcnow()
    if end_dt > now:
        end_dt = now

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = start_str

    folder = os.path.join(RAW_WEATHER_DIR, city_name)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{execution_date}.json")

    if os.path.exists(filename) and os.path.getsize(filename) > 100:
        print(f"[SKIP] Weather data already exists: {filename}")
        return

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_str,
        'end_date': end_str,
        'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,snowfall_sum,cloudcover_mean,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration',
        'timezone': 'GMT'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # This will raise an error for HTTP codes 4xx/5xx
        data = response.json()
        with open(filename, 'w') as f:
            json.dump(data, f)
    except requests.exceptions.Timeout:
        print(f"[ERROR] Request timeout for city {city_name}")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for city {city_name}: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


def fetch_daily_data(**context):
    cities = load_cities()

    # Use Airflow's execution_date, which is already in UTC
    execution_date = datetime.strptime(context['ds'], "%Y-%m-%d")

    today_utc = datetime.now(timezone.utc).date()

    # Skip if execution date is more recent than the last two days
    if execution_date.date() > today_utc - timedelta(days=2):
        print(f"[SKIP] {execution_date.date()} is too recent. Skipping.")
        raise AirflowSkipException("Execution date too recent for available data.")

    for city in cities:
        fetch_air_pollution(city, context['ds'])
        time.sleep(2)
        fetch_weather(city, context['ds'])
        time.sleep(2)
# Flatten and aggregate functions
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
        df['timestamp'] = pd.to_datetime(df['dt'], unit='s', errors='coerce')
    return df


def flatten_weather(json_data):
    daily_data = json_data.get('daily', {})
    records = []
    num_days = len(daily_data.get('time', []))

    for i in range(num_days):
        record = {}
        for key, values in daily_data.items():
            try:
                val = values[i]
                # Ensure scalar value
                if isinstance(val, (list, dict)):
                    record[key] = str(val)
                elif pd.isna(val):
                    record[key] = None
                else:
                    record[key] = val
            except Exception as e:
                record[key] = None  # fallback if index or type is bad

        record['lat'] = json_data.get('latitude')
        record['lon'] = json_data.get('longitude')
        records.append(record)

    df = pd.DataFrame(records)

    # Optional: convert columns with mixed types to string to avoid PySpark errors
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: str(x))

    df = df.fillna('')  # Optional: fill NaNs with empty string
    return df


def aggregate_pollution(df: pd.DataFrame) -> pd.DataFrame:
    # Extract hour and date from the timestamp
    df['hour'] = df['timestamp'].dt.hour
    # Convert from UTC to the desired timezone (UTC here, but can be adjusted as needed)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')  # Format to 'YYYY-MM-DD'

    # Filter data for hours between 6 AM and 10 PM
    df_daytime = df[(df['hour'] >= 6) & (df['hour'] <= 22)]

    if df_daytime.empty:
        return pd.DataFrame()

    # Define the components to aggregate
    components = ['co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3']

    # Calculate the mean of the components and round to 3 decimal places
    avg_components = df_daytime[components].mean().round(3).to_dict()  # Round to 3 decimals

    # Calculate the most common AQI value (mode)
    aqi_mode = df_daytime['aqi'].mode().iloc[0] if not df_daytime['aqi'].mode().empty else None

    # Prepare the final result, making sure the date is in the 'YYYY-MM-DD' format
    result = {
        **avg_components,
        'aqi_mode': aqi_mode,
        'lat': df_daytime['lat'].iloc[0],
        'lon': df_daytime['lon'].iloc[0],
        'time': df_daytime['date'].iloc[2]  # The date in 'YYYY-MM-DD' format
    }

    return pd.DataFrame([result])



def format_daily_data(**context):
    execution_date = datetime.strptime(context['ds'], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    today_utc = datetime.now(timezone.utc).date()
    if execution_date.date() > today_utc - timedelta(days=2):
        print(f"[SKIP] {execution_date.date()} is too recent. Skipping.")
        raise AirflowSkipException("Execution date too recent for available data.")

    execution_date = context['ds']
    print(f"[START] Formatting data for execution_date: {execution_date}")

    spark = SparkSession.builder.appName("FormatAndFlatten").master("local[*]").getOrCreate()
    print("[INFO] Spark session started")

    cities = load_cities()
    print(f"[INFO] Loaded {len(cities)} cities from config")

    for city in cities:
        city_name = city['name'].lower().replace(" ", "_")
        print(f"[PROCESSING] City: {city_name}")

        # === Pollution Data ===
        pollution_path = os.path.join(RAW_POLLUTION_DIR, city_name, f"{execution_date}.json")
        output_pollution_path = os.path.join(FORMATTED_POLLUTION_DIR, city_name, execution_date)

        if os.path.exists(pollution_path):
            print(f"[FOUND] Pollution file exists: {pollution_path}")
            with open(pollution_path, 'r') as f:
                json_data = json.load(f)
                df_pollution = flatten_pollution(json_data)
                print(f"[INFO] Flattened pollution data: {len(df_pollution)} rows")


                df_pollution = aggregate_pollution(df_pollution)
                # Round the numerical values to 3 decimal places

                print(f"[INFO] Aggregated pollution data: {len(df_pollution)} rows")

                if not df_pollution.empty:
                    df_pollution_spark = spark.createDataFrame(df_pollution)
                    df_pollution_spark = df_pollution_spark.withColumn("city_name", lit(city_name))
                    df_pollution_spark = df_pollution_spark.withColumn("population", lit(city['population']))
                    os.makedirs(os.path.dirname(output_pollution_path), exist_ok=True)
                    df_pollution_spark.write.mode("overwrite").parquet(output_pollution_path)
                    print(f"[OK] Formatted pollution for {city_name}")
                else:
                    print(f"[SKIP] No pollution data after aggregation for {city_name}")
        else:
            print(f"[MISSING] Pollution file does not exist for {city_name}")

        # === Weather Data ===
        weather_path = os.path.join(RAW_WEATHER_DIR, city_name, f"{execution_date}.json")
        output_weather_path = os.path.join(FORMATTED_WEATHER_DIR, city_name, execution_date)
        if os.path.exists(weather_path):
            print(f"[FOUND] Weather file exists: {weather_path}")
            with open(weather_path, 'r') as f:
                json_data = json.load(f)
                df_weather = flatten_weather(json_data)
                print(f"[INFO] Flattened weather data: {len(df_weather)} rows")

                # Round the numerical values to 3 decimal places


                if not df_weather.empty:
                    df_weather_spark = spark.createDataFrame(df_weather)
                    df_weather_spark = df_weather_spark.withColumn("city_name", lit(city_name))
                    df_weather_spark = df_weather_spark.withColumn("population", lit(city['population']))
                    os.makedirs(os.path.dirname(output_weather_path), exist_ok=True)
                    df_weather_spark.write.mode("overwrite").parquet(output_weather_path)
                    print(f"[OK] Formatted weather for {city_name}")
                else:
                    print(f"[SKIP] No weather data for {city_name}")
        else:
            print(f"[MISSING] Weather file does not exist for {city_name}")

    spark.stop()
    print("[DONE] Spark session closed")
def combine_daily_data(**context):
    execution_date = context['ds']
    spark = SparkSession.builder.appName("CombineDailyData").master("local[*]").getOrCreate()
    cities = load_cities()

    print(f"[START] Combining daily data for {execution_date}")

    for city in cities:
        city_name = city['name'].lower().replace(" ", "_")
        print(f"[COMBINING] City: {city_name}")

        pollution_path = os.path.join(FORMATTED_POLLUTION_DIR, city_name, execution_date)
        weather_path = os.path.join(FORMATTED_WEATHER_DIR, city_name, execution_date)
        combined_path = os.path.join(FORMATTED_COMBINED_DIR, city_name, execution_date)

        if not (os.path.exists(pollution_path) and os.path.exists(weather_path)):
            print(f"[SKIP] Missing pollution or weather file for {city_name} on {execution_date}")
            continue

        try:
            df_poll = spark.read.parquet(pollution_path).withColumn("city_name", trim(col("city_name")))
            df_weather = spark.read.parquet(weather_path).withColumn("city_name", trim(col("city_name")))

            df_weather = df_weather.drop("lat", "lon","population")  # Remove conflicting columns

            df_combined = df_poll.join(
                df_weather,
                on=["city_name", "time"],
                how="inner"
            )

            if not df_combined.rdd.isEmpty():
                os.makedirs(os.path.dirname(combined_path), exist_ok=True)
                df_combined.write.mode("overwrite").parquet(combined_path)
                print(f"[OK] Combined written: {combined_path}")
            else:
                print(f"[SKIP] Empty join result for {city_name} on {execution_date}")

        except Exception as e:
            print(f"[ERROR] Failed to combine {city_name} on {execution_date}: {e}")

    spark.stop()
    print(f"[DONE] Finished combining daily data for {execution_date}")
def index_daily_to_elasticsearch(**context):
    spark = SparkSession.builder.appName("IndexDailyES").master("local[*]").getOrCreate()
    es = Elasticsearch("http://localhost:9200")
    index_name = "environment_data"
    execution_date = context['ds']
    cities = load_cities()

    if not es.ping():
        print("[ERROR] Cannot connect to Elasticsearch.")
        return

    for city in cities:
        city_name = city['name'].lower().replace(" ", "_")
        file_path = os.path.join(FORMATTED_COMBINED_DIR, city_name, execution_date)

        if not os.path.exists(file_path):
            continue

        try:
            df = spark.read.parquet(file_path)
            records = df.toJSON().collect()

            for record in records:
                doc = json.loads(record)
                es.index(index=index_name, document=doc)

            print(f"[OK] Indexed {city_name}/{execution_date}")
        except Exception as e:
            print(f"[ERROR] Failed to index {city_name}/{execution_date}: {e}")

    spark.stop()
    print(f"[DONE] Finished indexing daily data for {execution_date}")
# # DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_daily_pollution_weather_dag',
    default_args=default_args,
    description='Daily fetch of air pollution and weather data for all cities',
    start_date=datetime(2025, 6, 5),
    schedule_interval='@daily',  # Run daily
    catchup=True,
    max_active_runs=1,
    tags=['pollution', 'weather', 'daily']
) as dag:

    fetch_daily_task = PythonOperator(
        task_id='fetch_daily_data_all_cities',
        python_callable=fetch_daily_data,
        provide_context=True
    )

    format_daily_task = PythonOperator(
        task_id='format_daily_data_all_cities',
        python_callable=format_daily_data,
        provide_context=True
    )

    combine_daily_task = PythonOperator(
        task_id='combine_daily_data_all_cities',
        python_callable=combine_daily_data,
        provide_context=True
    )
    index_daily = PythonOperator(
        task_id='index_daily_combined_to_elasticsearch',
        python_callable=index_daily_to_elasticsearch,
        provide_context=True
    )

    fetch_daily_task >> format_daily_task >> combine_daily_task