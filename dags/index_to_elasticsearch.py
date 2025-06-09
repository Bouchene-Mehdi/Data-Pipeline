from elasticsearch import Elasticsearch
import pandas as pd
import os
import json

COMBINED_DIR = '/home/ccd/airflow/data/formatted/combined'
ES_INDEX = 'pollution_weather_index'

def index_combined_data():
    es = Elasticsearch("http://localhost:9200")
    if not es.ping():
        raise Exception("Could not connect to Elasticsearch")

    for city in os.listdir(COMBINED_DIR):
        city_dir = os.path.join(COMBINED_DIR, city)
        for date in os.listdir(city_dir):
            path = os.path.join(city_dir, date)
            df = pd.read_parquet(path)

            for record in df.to_dict(orient='records'):
                record['city'] = city
                record['date'] = date
                es.index(index=ES_INDEX, document=record)

    print("[✔] Data successfully indexed to Elasticsearch.")
