# ETL Open-Meteo - Proyecto final (Consigna TP 2025)

Este repo contiene un pipeline ETL que extrae datos horarios de la API Open-Meteo,
los transforma y los carga en un Data Warehouse local (PostgreSQL en Docker). Está pensado
para cumplir la consigna del TP 2025 (Diplomatura en Cloud Data Engineering).

## Qué incluye (resumen)
- Extracción desde Open-Meteo (API temporal) usando `requests`.
- Transformaciones con `pandas` (tipado y docstrings).
- Guardado intermedio en **Parquet** (staging/data).
- Carga final en PostgreSQL (simula Redshift local).
- Orquestación con **Airflow** (Docker Compose).
- Tests unitarios con `pytest`.
- CI: GitHub Actions workflow (tests).
- Makefile para facilitar ejecución.
- Decorators de logging y uso de Airflow Variables (`latitude`, `longitude`).

## Estructura del repo
```
etl_open_meteo_final/
├─ dags/
│  └─ etl_weather_dag.py
├─ scripts/
│  ├─ fetch_weather.py
│  ├─ transform_weather.py
│  ├─ load_dw.py
│  └─ utils.py
├─ tests/
│  ├─ test_fetch_weather.py
│  └─ test_transform_weather.py
├─ docker/
│  └─ docker-compose.yml
├─ .github/
│  └─ workflows/ci.yml
├─ requirements.txt
├─ Makefile
├─ .env (example)
└─ README.md
```

## Quickstart (local con Docker)
1. Clonar el repo.
2. Copiar `.env` y editar si corresponde.
3. Levantar servicios:
   ```
   cd docker
   docker-compose up -d
   ```
4. Acceder a Airflow: http://localhost:8080
5. En Airflow -> Admin -> Variables, podés setear `latitude` y `longitude` (ej: -34.61, -58.38). Si no existen, se usan valores por defecto.
6. Ejecutar DAG `etl_weather_pipeline` desde UI o esperar la corrida diaria.
7. Ejecutar tests localmente:
   ```
   pip install -r requirements.txt
   pytest -q
   ```
8. Para detener:
   ```
   make stop
   ```

## Notas sobre Redshift
Este proyecto usa PostgreSQL local como entorno equivalente a Redshift. Las sentencias SQL están escritas de forma compatible para permitir una migración sencilla.

## Comandos útiles (Makefile)
- `make run`  -> levanta docker-compose
- `make test` -> corre pytest
- `make stop` -> baja los servicios

