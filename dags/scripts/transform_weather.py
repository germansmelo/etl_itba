import pandas as pd

def transform_weather(df):
    "Limpia y enriquece los datos meteorológicos."
    # Elimina filas sin valor de temperatura
    df = df.dropna(subset=["temperature"]).copy()

    # Redondea las columnas numéricas a un decimal
    for col in ["temperature", "humidity", "wind_speed"]:
        df[col] = df[col].round(1)

    # Crea una categoría de temperatura (frío, templado, cálido)
    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-50, 10, 25, 50],
        labels=["Frío", "Templado", "Cálido"]
    )

    # Agrega una bandera para indicar viento fuerte
    df["high_wind_flag"] = (df["wind_speed"] > 20).astype(int)

    return df

