from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys


from scripts.fetch_weather import fetch_weather, weather_to_df
from scripts.transform_weather import transform_weather
from scripts.load_dw import create_tables, load_weather, save_staging_parquet
import pandas as pd

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_fetch(ti):
    lat = float(Variable.get("latitude", default_var=-34.61))
    lon = float(Variable.get("longitude", default_var=-58.38))
    data = fetch_weather(latitude=lat, longitude=lon)
    ti.xcom_push(key="raw_weather", value=data)

def task_transform(ti):
    raw = ti.xcom_pull(task_ids="fetch", key="raw_weather")
    df = weather_to_df(raw)
    df_transformed = transform_weather(df)
    ti.xcom_push(key="weather_df", value=df_transformed.to_dict(orient="records"))

def task_load(ti):
    create_tables()
    records = ti.xcom_pull(task_ids="transform", key="weather_df")
    df = pd.DataFrame(records)
    # Save staging parquet (avoid storing huge data in XCom)
    save_staging_parquet(df, path="/opt/airflow/data/staging/weather.parquet")
    load_weather(df)

with DAG(
    "etl_weather_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:
    fetch = PythonOperator(task_id="fetch", python_callable=task_fetch)
    transform = PythonOperator(task_id="transform", python_callable=task_transform)
    load = PythonOperator(task_id="load", python_callable=task_load)

    fetch >> transform >> load
