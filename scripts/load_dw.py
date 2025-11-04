from typing import Optional
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/weather_dw")
engine = create_engine(DB_URL, pool_pre_ping=True)

def create_tables() -> None:
    """Create dimension and fact tables if they do not exist."""
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
    with engine.begin() as conn:
        for stmt in ddl.strip().split(';'):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

def save_staging_parquet(df: pd.DataFrame, path: Optional[str] = "data/staging/weather.parquet") -> None:
    """Save transformed data to parquet for staging area."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Saved staging parquet to {path}")

def load_weather(df: pd.DataFrame) -> None:
    """Load transformed dataframe into the data warehouse (Postgres)."""
    with engine.begin() as conn:
        loc = {
            "city": df["city"].iloc[0],
            "latitude": float(df["latitude"].iloc[0]) if df["latitude"].notnull().any() else None,
            "longitude": float(df["longitude"].iloc[0]) if df["longitude"].notnull().any() else None
        }
        result = conn.execute(
            text("""INSERT INTO dim_location (city, latitude, longitude)
            VALUES (:city, :latitude, :longitude)
            ON CONFLICT (city) DO NOTHING
            RETURNING id
            """), loc
        )
        row = result.first()
        location_id = row[0] if row else conn.execute(
            text("SELECT id FROM dim_location WHERE city=:city"), {"city": loc["city"]}
        ).scalar()

        df_to_insert = df[[
            "time", "temperature", "humidity", "wind_speed", "temp_category", "high_wind_flag", "date_extracted"
        ]].copy()
        df_to_insert["location_id"] = location_id
        # Use pandas to_sql for simplicity
        df_to_insert.to_sql("fact_weather", engine, if_exists="append", index=False)
