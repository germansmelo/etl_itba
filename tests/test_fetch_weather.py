from docker.dags.scripts.fetch_weather import fetch_weather, weather_to_df

import pytest

def test_fetch_weather_basic():
    data = fetch_weather()
    assert "hourly" in data
    assert "temperature_2m" in data["hourly"]

def test_weather_to_df_shape():
    data = fetch_weather()
    df = weather_to_df(data)
    assert all(col in df.columns for col in ["temperature", "humidity", "wind_speed"])
    assert not df.empty
