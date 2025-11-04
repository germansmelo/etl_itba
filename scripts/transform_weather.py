from typing import Any
import pandas as pd

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich weather data.

    Args:
        df (pd.DataFrame): Raw weather dataframe.

    Returns:
        pd.DataFrame: Transformed dataframe with derived columns.
    """
    df = df.copy()
    # Drop rows without temperature
    df = df.dropna(subset=["temperature"])
    # Round numeric columns
    df["temperature"] = df["temperature"].round(1)
    df["humidity"] = df["humidity"].round(1)
    df["wind_speed"] = df["wind_speed"].round(1)
    # Derived category
    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-50, 10, 25, 50],
        labels=["Cold", "Mild", "Hot"]
    )
    # Example additional transform: flag high wind
    df["high_wind_flag"] = (df["wind_speed"] > 20).astype(int)
    return df
