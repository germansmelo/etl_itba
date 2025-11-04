import pandas as pd
from scripts.transform_weather import transform_weather

def test_transform_weather_adds_category():
    df = pd.DataFrame({
        "time": ["2025-11-04T10:00"],
        "temperature": [20.3],
        "humidity": [60.0],
        "wind_speed": [10.0],
        "city": ["Buenos Aires"],
        "latitude": [-34.61],
        "longitude": [-58.38],
        "date_extracted": ["2025-11-04T10:00"]
    })
    transformed = transform_weather(df)
    assert "temp_category" in transformed.columns
    assert transformed.iloc[0]["temp_category"] in ["Cold", "Mild", "Hot"]
    assert "high_wind_flag" in transformed.columns
