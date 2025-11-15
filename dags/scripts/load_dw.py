import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Define la URL de conexión a la base de datos (usa una por defecto si no existe en el .env)
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/weather_dw")

# Crea el motor de conexión con SQLAlchemy
engine = create_engine(DB_URL, pool_pre_ping=True)

def create_tables():
# Crea las tablas de dimensiones y hechos si no existen.
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_location (
        id SERIAL PRIMARY KEY,
        city TEXT UNIQUE,
        latitude FLOAT,
        longitude FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_weather (
        id SERIAL PRIMARY KEY,
        location_id INTEGER REFERENCES dim_location(id),
        time TIMESTAMP,
        temperature FLOAT,
        humidity FLOAT,
        wind_speed FLOAT,
        temp_category TEXT,
        high_wind_flag INTEGER,
        date_extracted TIMESTAMP
    );
    """
    # Ejecuta cada sentencia CREATE TABLE
    with engine.begin() as conn:
        for stmt in ddl.strip().split(';'):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

def save_staging_parquet(df, path="data/staging/weather.parquet"):
# Guarda los datos transformados en formato Parquet en la carpeta de staging.
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Archivo Parquet guardado en: {path}")

def load_weather(df):
# Carga los datos transformados en el data warehouse (PostgreSQL).
    with engine.begin() as conn:
        # Inserta o recupera la ubicación
        loc = {
            "city": df["city"].iloc[0],
            "latitude": float(df["latitude"].iloc[0]) if df["latitude"].notnull().any() else None,
            "longitude": float(df["longitude"].iloc[0]) if df["longitude"].notnull().any() else None
        }

        result = conn.execute(
            text("""
                INSERT INTO dim_location (city, latitude, longitude)
                VALUES (:city, :latitude, :longitude)
                ON CONFLICT (city) DO NOTHING
                RETURNING id
            """),
            loc
        )

        # Si no se insertó, busca el ID existente
        row = result.first()
        location_id = row[0] if row else conn.execute(
            text("SELECT id FROM dim_location WHERE city=:city"), {"city": loc["city"]}
        ).scalar()

        # Prepara el DataFrame para insertar en fact_weather
        df_to_insert = df[[
            "time", "temperature", "humidity", "wind_speed", "temp_category", "high_wind_flag", "date_extracted"
        ]].copy()
        df_to_insert["location_id"] = location_id

        # Inserta los datos en la tabla de hechos
        df_to_insert.to_sql("fact_weather", engine, if_exists="append", index=False)
