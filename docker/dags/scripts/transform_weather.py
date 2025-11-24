import pandas as pd
from airflow.models import Variable

def transform_weather(df):

    # Elimina filas sin valor de temperatura
    df = df.dropna(subset=["temperature"]).copy()

    # Redondea las columnas numéricas a un decimal
    for col in ["temperature", "humidity", "wind_speed"]:
        df[col] = df[col].round(1)

    # Bins de temperatura (Parametrizables)
    default_bins = "[-50, 10, 25, 50]"
    default_labels = "['Frío', 'Templado', 'Cálido']"

    # Airflow Variables permiten cambiar fácilmente los bins desde la UI
    bins = Variable.get("temp_bins", default_var=default_bins)
    labels = Variable.get("temp_labels", default_var=default_labels)

    # Convertir texto → listas reales
    bins = eval(bins)
    labels = eval(labels)

    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    wind_threshold = float(Variable.get("wind_threshold", default_var="20"))

    df["high_wind_flag"] = (df["wind_speed"] > wind_threshold).astype(int)

   
    df["is_raining"] = df.get("precipitation", 0) > 0

    return df

