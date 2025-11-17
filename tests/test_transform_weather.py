import pandas as pd
from docker.dags.scripts.transform_weather import transform_weather

def test_transform_weather_adds_category_and_flag():
    # Crea un DataFrame de ejemplo similar al esperado
    df = pd.DataFrame({
        "time": ["2025-11-04T10:00"],
        "temperature": [20.3],
        "humidity": [60.0],
        "wind_speed": [10.0],
        "precipitation": [1.2],
        "city": ["Buenos Aires"],
        "latitude": [-34.61],
        "longitude": [-58.38],
        "date_extracted": ["2025-11-04T10:00"]
    })

    # Aplica la función transform_weather
    transformed = transform_weather(df)

    # Verifica que la columna de categoría se haya agregado
    assert "temp_category" in transformed.columns
    # Verifica que el valor de la categoría esté dentro de las etiquetas esperadas
    assert transformed.iloc[0]["temp_category"] in ["Frío", "Templado", "Cálido"]

    # Verifica que se haya agregado la bandera de viento fuerte
    assert "high_wind_flag" in transformed.columns
    # Dado que el viento es 10.0 (no fuerte), el valor debería ser 0
    assert transformed.iloc[0]["high_wind_flag"] == 0

    # Verifica que los valores numéricos estén redondeados a un decimal
    assert transformed["temperature"].iloc[0] == 20.3
    assert transformed["humidity"].iloc[0] == 60.0
    assert transformed["wind_speed"].iloc[0] == 10.0

    # Verifica columna 'is_raining'
    assert "is_raining" in transformed.columns, "La columna 'is_raining' no fue creada."
    assert bool(transformed["is_raining"].iloc[0]) is True, "El valor de 'is_raining' debería ser True cuando hay precipitación."
