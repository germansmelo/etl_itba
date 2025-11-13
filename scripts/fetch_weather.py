import requests
import pandas as pd
from datetime import datetime, timezone
from .utils import log_step


@log_step
def fetch_weather(latitude=-34.61, longitude=-58.38):
 # Obtiene datos meteorológicos horarios desde la API de Open-Meteo.
    # Construye la URL de consulta con las coordenadas y variables deseadas
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    )

    # Hace la solicitud a la API y valida la respuesta
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()

    # Devuelve los datos en formato JSON
    return resp.json()

def weather_to_df(data):
 # Convierte el JSON de Open-Meteo en un DataFrame de pandas.
    hourly = data['hourly']

    # Crea el DataFrame con las variables principales
    df = pd.DataFrame({
        'time': hourly['time'],
        'temperature': hourly['temperature_2m'],
        'humidity': hourly['relative_humidity_2m'],
        'wind_speed': hourly['wind_speed_10m']
    })

    # Ajusta formatos y agrega información adicional
    df['time'] = pd.to_datetime(df['time'])
    df['city'] = data.get('timezone', 'Desconocido')
    df['latitude'] = data.get('latitude')
    df['longitude'] = data.get('longitude')
    df['date_extracted'] = datetime.now(timezone.utc)

    return df
