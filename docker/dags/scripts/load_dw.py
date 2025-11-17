import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carga las variables del entorno
load_dotenv()

DB_URL = os.getenv(
    "DW_DB_URL",
    "postgresql+psycopg2://postgres:postgres@dw-postgres:5432/weather_dw"
)

# Crea el motor
engine = create_engine(DB_URL, pool_pre_ping=True)


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

    try:
        with engine.begin() as conn:
            statements = [stmt.strip() for stmt in ddl.strip().split(';') if stmt.strip()]
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Tablas creadas/verificadas exitosamente")
    except Exception as e:
        logger.error(f"Error al crear tablas: {e}")
        raise


def save_staging_parquet(df, path="data/staging/weather.parquet"):
    """Guarda el archivo Parquet en staging."""
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Archivo Parquet guardado en: {path}")
    except Exception as e:
        logger.error(f"Error al guardar Parquet: {e}")
        raise


def get_or_create_location(conn, city, latitude, longitude):
    """
    Obtiene o crea una ubicación en dim_location.
    Retorna el location_id.
    """
    try:
        # Intenta insertar, si ya existe actualiza y retorna el id
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
        
        # Si no se retorna nada (puede pasar en algunas versiones de PostgreSQL)
        # hacemos un SELECT explícito
        if location_id is None:
            location_id = conn.execute(
                text("SELECT id FROM dim_location WHERE city = :city"),
                {"city": city}
            ).scalar()
        
        logger.info(f"Location ID obtenido: {location_id} para ciudad: {city}")
        return location_id
        
    except Exception as e:
        logger.error(f"Error al obtener/crear location: {e}")
        raise


def load_weather(df):
    """Inserta datos en el DW usando dim_location y fact_weather."""
    
    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para cargar")
        return

    # Validar columnas requeridas
    required_cols = ["city", "latitude", "longitude", "time", "temperature", 
                     "humidity", "wind_speed", "temp_category", 
                     "high_wind_flag", "date_extracted"]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Faltan columnas requeridas: {missing_cols}")

    try:
        with engine.begin() as conn:
            # Obtener información de ubicación (primera fila, asumiendo misma ciudad)
            city = df["city"].iloc[0]
            latitude = float(df["latitude"].iloc[0])
            longitude = float(df["longitude"].iloc[0])
            
            # Obtener o crear location_id
            location_id = get_or_create_location(conn, city, latitude, longitude)
            
            # Preparar datos para fact_weather
            df_to_insert = df[[
                "time", "temperature", "humidity", "wind_speed",
                "temp_category", "high_wind_flag", "date_extracted"
            ]].copy()
            
            df_to_insert["location_id"] = location_id
            
            # Insertar en fact_weather (fuera de la transacción actual para usar to_sql)
            # Nota: to_sql maneja su propia conexión
            rows_before = conn.execute(text("SELECT COUNT(*) FROM fact_weather")).scalar()
        
        # Insertar datos usando to_sql con manejo de duplicados
        df_to_insert.to_sql(
            "fact_weather", 
            engine, 
            if_exists="append", 
            index=False,
            method="multi"  # Más eficiente para múltiples filas
        )
        
        with engine.begin() as conn:
            rows_after = conn.execute(text("SELECT COUNT(*) FROM fact_weather")).scalar()
            rows_inserted = rows_after - rows_before
        
        logger.info(f"Insertadas {rows_inserted} filas en fact_weather para ciudad {city}")
        
    except Exception as e:
        logger.error(f"Error al cargar datos al DW: {e}")
        raise


def main():
    """Función principal para ejecutar el proceso de carga."""
    try:
        logger.info("Iniciando proceso de carga al DW")
        
        # Crear tablas si no existen
        create_tables()
        
        # Ejemplo: cargar datos desde parquet procesado
        input_path = "data/processed/weather_transformed.parquet"
        
        if not Path(input_path).exists():
            logger.error(f"Archivo no encontrado: {input_path}")
            raise FileNotFoundError(f"No se encuentra el archivo: {input_path}")
        
        df = pd.read_parquet(input_path)
        logger.info(f"Cargadas {len(df)} filas desde {input_path}")
        
        # Cargar al DW
        load_weather(df)
        
        # Opcional: guardar copia en staging
        save_staging_parquet(df)
        
        logger.info("Proceso de carga completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
        raise


if __name__ == "__main__":
    main()