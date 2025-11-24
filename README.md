# ETL Weather

Este proyecto implementa un pipeline ETL para obtener datos meteorológicos, procesarlos y almacenarlos en una base de datos relacional. Está diseñado para ejecutarse mediante Apache Airflow y utiliza Docker para facilitar la orquestación y puesta en marcha del entorno.

# Contenido del Repositorio


```
etl_itba/
├── dags/                  # DAG de Airflow para orquestar el pipeline
├── scripts/               # Scripts Python con funciones ETL
├── tests/                 # Pruebas unitarias con Pytest
├── docker/                # Configuración para levantar la infraestructura con Docker
├── config/                # Configuración de Airflow (e.g. airflow.cfg, logging)
├── logs/                  # Logs generados durante la ejecución del pipeline
├── plugins/               # Plugins personalizados para Airflow (vacío por defecto)
├── .github/
│   └── workflows/
│       └── ci.yml         # Workflow de GitHub Actions para testeo automático
├── Makefile               # Comandos útiles para desarrollo y despliegue
├── requirements.txt       # Dependencias del proyecto
├── pytest.ini             # Configuración de Pytest
├── .gitignore             # Archivos a ignorar por Git
└── README.md              # Este archivo
```

# API

Datos obtenidos desde la API de Open-Meteo

El endpoint utilizado permite pedir datos horarios.

La solicitud trae:

- temperature_2m
- relative_humidity_2m
- wind_speed_10m
- precipitation


# Objetivo

Automatizar la ingesta y procesamiento de datos meteorológicos desde un servicio público (API de Open-Meteo), transformarlos en un DataFrame y cargarlos en una base de datos PostgreSQL, todo mediante un DAG de Airflow.

El pipeline realiza:

1. Extracción desde la API Open-Meteo

- Temperatura
- Humedad
- Velocidad del viento
- Precipitación

2. Transformación con reglas de negocio configurables:

- Redondeo de valores
- Creación de categorías (bins) para la temperatura
- Umbral de viento fuerte
- Indicador de lluvia
- Enriquecimiento con ciudad, latitud y longitud

3. Carga en un Data Warehouse relacional con modelo estrella:

- Dimensión: dim_location
- Tabla de hechos: fact_weather
- Orquestación mediante Airflow

# Modelo de Datos del Data Warehouse

# dim_location

| Campo     | Tipo  | Descripción |
|-----------|--------|-------------|
| id        | SERIAL | Identificador único |
| city      | TEXT UNIQUE | Ciudad asignada desde Airflow |
| latitude  | FLOAT  | Latitud |
| longitude | FLOAT  | Longitud |

# fact_weather


| Campo          | Tipo       | Descripción |
|----------------|------------|-------------|
| id             | SERIAL PK  | Identificador |
| location_id    | FK         | Relación con dim_location |
| time           | TIMESTAMP  | Timestamp del dato |
| temperature    | FLOAT      | Temperatura (°C) |
| humidity       | FLOAT      | Humedad (%) |
| wind_speed     | FLOAT      | Velocidad del viento |
| precipitation  | FLOAT      | Precipitación (mm) |
| temp_category  | TEXT       | Categoría según bins |
| high_wind_flag | INT        | 1 si supera el umbral |
| date_extracted | TIMESTAMP  | Fecha de ingesta |



# Tecnologías utilizadas

- **Python 3.12+**
- **Apache Airflow** (ejecutándose en Docker con Redis y PostgreSQL)
- **Pandas**
- **SQLAlchemy**
- **Requests**
- **Pytest**
- **Docker Compose**


# Instalación y ejecución

# Prerequisitos

- Docker y Docker Compose instalados
- Python 3.10 o superior
- Make (opcional, para usar los comandos del Makefile)

# Clonar el repositorio

```bash
git clone https://github.com/your-username/etl_itba.git
cd etl_itba
```

# Crear y configurar el entorno

Instala las dependencias de desarrollo:

```bash
pip install -r requirements.txt
```

# Levantar la infraestructura con Docker

```bash
docker compose up -d
```

Esto levantará:
- Airflow Webserver en http://localhost:8080
- PostgreSQL en el puerto 5432
- Redis en el puerto 6379

También podés utilizar el Makefile:

```bash
make up
```


# Ejecutar los DAGs

Accede a la UI de Airflow y activa el DAG `etl_weather_dag` para procesar los datos.

# Testing

Este proyecto incluye pruebas unitarias escritas con Pytest para validar las funciones principales del ETL:

```bash
pytest
```

Los tests se encuentran en la carpeta "tests" y comprueban:

- Que se obtienen los datos desde la API
- Que las transformaciones son correctas
- Que el DataFrame resultante tiene las columnas esperadas

# CI/CD con GitHub Actions

El repositorio contiene un workflow en `.github/workflows/test.yml` que:

- Instala dependencias
- Ejecuta los tests
- Verifica el formato del código
- Asegura la calidad al hacer un Pull Request


# Workflow de Pull Request

Para enviar cambios:
1. Crear una rama nueva
2. Hacer los cambios
3. Realizar un Pull Request descriptivo hacia `main`
4. Verificar que los tests pasen en CI
5. Fusionar una vez aprobado


# Estructura del Código

# /scripts

Contiene los módulos Python con las principales funciones ETL:

- fetch_weather.py: Descarga los datos desde la API Open-Meteo
- transform_weather.py: Limpia y transforma los datos
- load_weather.py: Inserta los datos en PostgreSQL

Cada función incluye decoradores de logging para trazabilidad.

# transform_weather.py

# Bins y Umbrales Parametrizables

Variables de Airflow completamente configurables desde la UI.

Bins de temperatura  ([-50, 10, 25, 50])
Etiquetas  para cada bin (["Frío", "Templado", "Cálido"])
Umbral de viento fuerte  (> 20 km/h)

Se parametrizó completamente estos valores, permitiendo modificarlos desde Airflow sin cambiar ningún script.

Estas variables pueden editarse desde:

Airflow UI → Admin → Variables

# /dags

Contiene el archivo `etl_weather_dag.py` con el orquestador de Airflow que encadena las etapas del ETL de datis meteorológicos provenientes de la API pública Open-Meteo.

El DAG ejecuta automáticamente las tres etapas del ETL:

Extract → Transform → Load

Cada tarea usa un operador PythonOperator y está envuelta por un decorador de logging para trazabilidad.

El DAG también:

Loguea inicios y finales de cada tarea

Maneja fallos con retry

- Tareas del DAG 

1. fetch – Extracción de datos desde la API
La primera tarea del DAG obtiene la información meteorológica desde Open-Meteo y la deja lista para ser procesada en las siguientes etapas.

- Obtiene latitud, longitud y la ciudad desde Variables de Airflow, configurables desde la UI (Admin → Variables).
Esto permite cambiar la ubicación sin modificar el código.
- Llama a fetch_weather(), que construye la URL y consulta la API de Open-Meteo usando las coordenadas obtenidas.
- Convierte la respuesta en un DataFrame inicial mediante weather_to_df(), asignando la ciudad definida por el usuario (en vez de usar el campo timezone de la API, que no representa una ciudad real).
- Guarda el DataFrame crudo en formato Parquet en la carpeta staging, lo cual permite trabajar con datasets grandes y evita almacenar datos en XCom.
- Envía por XCom solo el path del archivo Parquet, no los datos, siguiendo buenas prácticas de Airflow para evitar saturar la base interna de metadatos.

2. transform – Limpieza y enriquecimiento

- Lee el Parquet generado en fetch

Ejecuta transformaciones:

- Limpieza de nulos
- Normalización de columnas

Agrega columnas:

- category (según weathercode)
- is_extreme (flag de clima severo)

Sobrescribe el archivo weather.parquet con los datos transformados

3. load – Carga en PostgreSQL

Funciones:

- Garantiza la creación de tabla fact_weather si no existe.
- Lee el parquet transformado.
- Inserta los registros en la base weather_dw.

# Idempotencia del Proceso ETL

La idempotencia es un principio fundamental en procesos ETL:

Ejecutar el pipeline varias veces debe producir siempre el mismo resultado, sin duplicar ni corromper datos.

- En este proyecto el ETL puede ejecutarse más de una vez por día (manual o automática).

- La tabla fact_weather tiene la restricción UNIQUE(location_id, time).

- El proceso debe poder reescribirse con seguridad si hay fallos, reintentos o reprocesos.


# /pgAdmin Cómo usar pgAdmin

pgAdmin está disponible para visualizar los datos cargados.

- Acceder a pgAdmin
http://localhost:80


- Credenciales configuradas en Docker:

Email: admin@admin.com

Password: admin

- Conectar al servidor PostgreSQL

Click en Add New Server

- Completar:

General → Name:
weather_dw

- Connection:

Host: postgres

Username: postgres

Password: postgres

Database: weather_dw

- Visualizar los datos

Navegar a:

Servers → weather_dw → Databases → weather_dw → Schemas → public → Tables → fact_weather

Luego:

Right click → View/Edit Data → All Rows

# Variables de Airflow

2. Valores recomendados por defecto

Se recomienda configurar los siguientes valores:

# Variables obligatorias
city = "Buenos Aires"
latitude = -34.61
longitude = -58.38

# Pasos para configurarlas en Airflow

1. Abrir Airflow:  
   http://localhost:8080  
2. Ir a:  
   **Admin → Variables**  
3. Clic en **+ Add a new record**  
4. Crear cada variable con:  
   - **Key** = nombre  
   - **Val** = valor  
5. Guardar  
6. Repetir para todas las variables

Ejemplo final esperado:

| Key            | Val |
|----------------|-----|
| city           | "Buenos Aires" |
| latitude       | -34.61 |
| longitude      | -58.38 |
| temp_bins      | [-50, 10, 25, 50] |
| temp_labels    | ["Frío","Templado","Cálido"] |
| wind_threshold | 20 |

---


# Configuración importante

# Variables de entorno

Este proyecto utiliza un archivo .env para centralizar todas las variables necesarias para levantar correctamente:

- Airflow
- PostgreSQL (Data Warehouse)
- pgAdmin
- Servicios Docker

Estas variables deben estar disponibles antes de ejecutar cualquier comando de Docker o Airflow.

# Usuario y grupo con los que correrá Airflow dentro de Docker
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Conexión al Data Warehouse (PostgreSQL)
DW_DB_URL=postgresql+psycopg2://postgres:postgres@dw-postgres:5432/weather_dw
