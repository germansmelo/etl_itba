from typing import Any, Dict
import requests
import pandas as pd
from datetime import datetime
from .utils import log_step

@log_step
def fetch_weather(latitude: float = -34.61, longitude: float = -58.38) -> Dict[str, Any]:
    """Fetch hourly weather data from Open-Meteo for given coordinates.

    Args:
        latitude (float): Latitude coordinate.
        longitude (float): Longitude coordinate.

    Returns:
        Dict[str, Any]: JSON response from Open-Meteo.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def weather_to_df(data: Dict[str, Any]) -> pd.DataFrame:
    """Convert Open-Meteo JSON into a pandas DataFrame."""
    hourly = data['hourly']
    df = pd.DataFrame({
        'time': hourly['time'],
        'temperature': hourly['temperature_2m'],
        'humidity': hourly['relative_humidity_2m'],
        'wind_speed': hourly['wind_speed_10m']
    })
    df['time'] = pd.to_datetime(df['time'])
    df['city'] = data.get('timezone', 'Unknown')
    df['latitude'] = data.get('latitude')
    df['longitude'] = data.get('longitude')
    df['date_extracted'] = datetime.utcnow()
    return df
