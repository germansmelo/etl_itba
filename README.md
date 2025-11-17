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


# Objetivo

Automatizar la ingesta y procesamiento de datos meteorológicos desde un servicio público (API de Open-Meteo), transformarlos en un DataFrame y cargarlos en una base de datos PostgreSQL, todo mediante un DAG de Airflow.


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


# structura del Código

# /scripts

Contiene los módulos Python con las principales funciones ETL:

- fetch_weather.py: Descarga los datos desde la API Open-Meteo
- transform_weather.py: Limpia y transforma los datos
- load_weather.py: Inserta los datos en PostgreSQL

Cada función incluye decoradores de logging para trazabilidad.

# /dags

Contiene el archivo `etl_weather_dag.py` con el orquestador de Airflow que encadena las etapas del ETL.


# Configuración importante

# Variables de entorno

Este proyecto utiliza variables de entorno para conectar con la base de datos. Puedes crear un archivo `.env` con los siguientes datos:

DB_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/weather_dw
