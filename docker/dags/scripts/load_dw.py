import os
from sqlalchemy import create_engine, text
import pandas as pd
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_engine():
    """Devuelve una instancia de SQLAlchemy Engine (lazy loading)."""
    DB_URL = os.getenv(
        "DW_DB_URL",
        "postgresql+psycopg2://postgres:postgres@dw-postgres:5432/weather_dw"
    )
    return create_engine(DB_URL, pool_pre_ping=True)


def create_tables():
    """Crea las tablas dim_location y fact_weather si no existen."""
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_location (
        id SERIAL PRIMARY KEY,
        city TEXT UNIQUE NOT NULL,
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_dim_location_city ON dim_location(city);

    CREATE TABLE IF NOT EXISTS fact_weather (
        id SERIAL PRIMARY KEY,
        location_id INTEGER NOT NULL REFERENCES dim_location(id),
        time TIMESTAMP NOT NULL,
        temperature FLOAT,
        humidity FLOAT,
        wind_speed FLOAT,
        temp_category TEXT,
        high_wind_flag INTEGER,
        date_extracted TIMESTAMP NOT NULL,
        UNIQUE(location_id, time)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_weather_location_time 
        ON fact_weather(location_id, time);
    """

    engine = get_engine()
    with engine.begin() as conn:
        statements = [stmt.strip() for stmt in ddl.strip().split(';') if stmt.strip()]
        for stmt in statements:
            conn.execute(text(stmt))

    logger.info("Tablas creadas/verificadas exitosamente")


def save_staging_parquet(df, path="data/staging/weather.parquet"):
    """Guarda el archivo Parquet en staging."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Archivo Parquet guardado en: {path}")


def get_or_create_location(conn, city, latitude, longitude):
    """
    Obtiene o crea una ubicación en dim_location.
    Retorna el location_id.
    """

    result = conn.execute(
        text("""
            INSERT INTO dim_location (city, latitude, longitude)
            VALUES (:city, :latitude, :longitude)
            ON CONFLICT (city) DO UPDATE
                SET latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            RETURNING id;
        """),
        {"city": city, "latitude": latitude, "longitude": longitude}
    )

    location_id = result.scalar()

    if location_id is None:
        location_id = conn.execute(
            text("SELECT id FROM dim_location WHERE city = :city"),
            {"city": city}
        ).scalar()

    logger.info(f"Location ID obtenido: {location_id} para ciudad: {city}")
    return location_id


def load_weather(df):
    """Inserta datos en el DW."""

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para cargar")
        return

    required_cols = [
        "city", "latitude", "longitude", "time", "temperature",
        "humidity", "wind_speed", "temp_category",
        "high_wind_flag", "date_extracted"
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Faltan columnas requeridas: {missing_cols}")

    # Convertir times a datetime si vienen como string
    df["time"] = pd.to_datetime(df["time"])
    city = df["city"].iloc[0]
    latitude = float(df["latitude"].iloc[0])
    longitude = float(df["longitude"].iloc[0])

    engine = get_engine()

    with engine.begin() as conn:
        location_id = get_or_create_location(conn, city, latitude, longitude)

        # Obtener rango de fechas del lote
        start_time = df["time"].min()
        end_time = df["time"].max()

        # Borrar datos duplicados (IDEMPOTENCIA)
        conn.execute(
            text("""
                DELETE FROM fact_weather 
                WHERE location_id = :loc
                  AND time BETWEEN :start AND :end
            """),
            {"loc": location_id, "start": start_time, "end": end_time}
        )
        logger.info("Datos previos eliminados para garantizar idempotencia.")

    # Insertar datos nuevos
    df_to_insert = df[[
        "time", "temperature", "humidity", "wind_speed",
        "temp_category", "high_wind_flag", "date_extracted"
    ]].copy()

    df_to_insert["location_id"] = location_id

    engine = get_engine()
    df_to_insert.to_sql(
        "fact_weather",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    logger.info(f"Insertadas {len(df_to_insert)} filas idempotentes para ciudad {city}")