from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

from scripts.fetch_weather import fetch_weather, weather_to_df
from scripts.transform_weather import transform_weather
from scripts.load_dw import create_tables, load_weather, save_staging_parquet

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

STAGING_PATH = "/opt/airflow/data/staging/weather.parquet"

def task_fetch(ti):
    lat = float(Variable.get("latitude", default_var=-34.61))
    lon = float(Variable.get("longitude", default_var=-58.38))
    city = Variable.get("city_name", default_var="Buenos Aires")
    raw = fetch_weather(latitude=lat, longitude=lon)
    df = weather_to_df(raw, city_name=city)

    # Guardar como staging
    save_staging_parquet(df, STAGING_PATH)

    # Pasar solo el path por XCom
    ti.xcom_push(key="staging_path", value=STAGING_PATH)

def task_transform(ti):
    path = ti.xcom_pull(task_ids="fetch", key="staging_path")

    df = pd.read_parquet(path)
    df_trans = transform_weather(df)

    # Sobrescribir staging transformado
    save_staging_parquet(df_trans, STAGING_PATH)

def task_load(ti):
    create_tables()

    df = pd.read_parquet(STAGING_PATH)
    load_weather(df)


with DAG(
    "etl_weather_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    fetch = PythonOperator(task_id="fetch", python_callable=task_fetch)
    transform = PythonOperator(task_id="transform", python_callable=task_transform)
    load = PythonOperator(task_id="load", python_callable=task_load)

    fetch >> transform >> load
