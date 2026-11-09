"""Transformación (T de ETL): une los CSV de World Happiness 2015-2019 en un esquema unificado."""
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_PATH = PROCESSED_DIR / "happiness_unified.csv"

YEARS = [2015, 2016, 2017, 2018, 2019]

# Nombre original de cada columna -> nombre del esquema unificado
COLUMN_MAP = {
    "Country": "country", "Country or region": "country",
    "Happiness Score": "score", "Happiness.Score": "score", "Score": "score",
    "Economy (GDP per Capita)": "gdp", "Economy..GDP.per.Capita.": "gdp", "GDP per capita": "gdp",
    "Family": "family", "Social support": "family",
    "Health (Life Expectancy)": "health", "Health..Life.Expectancy.": "health",
    "Healthy life expectancy": "health",
    "Freedom": "freedom", "Freedom to make life choices": "freedom",
    "Generosity": "generosity",
    "Trust (Government Corruption)": "corruption", "Trust..Government.Corruption.": "corruption",
    "Perceptions of corruption": "corruption",
}
UNIFIED_COLUMNS = ["country", "year", "score", "gdp", "family", "health", "freedom", "generosity", "corruption"]

# Mismo país escrito de forma distinta según el año
COUNTRY_MAP = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "North Cyprus": "Northern Cyprus",
    "Macedonia": "North Macedonia",
    "Somaliland Region": "Somaliland region",
}


def load_and_harmonize() -> pd.DataFrame:
    """Lee cada año, estandariza nombres de columnas y los une en una sola tabla."""
    frames = []
    for year in YEARS:
        df = pd.read_csv(RAW_DIR / f"{year}.csv").rename(columns=COLUMN_MAP)
        df["year"] = year
        frames.append(df[UNIFIED_COLUMNS])
    return pd.concat(frames, ignore_index=True)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza países, elimina filas con nulos y fija los tipos de datos."""
    df = df.copy()
    df["country"] = df["country"].str.strip().replace(COUNTRY_MAP)

    before = len(df)
    df = df.dropna()
    print(f"Filas eliminadas por nulos: {before - len(df)}")

    df = df.astype({"country": "string", "year": "int64"})
    return df.sort_values(["year", "country"]).reset_index(drop=True)


def main() -> None:
    unified = clean(load_and_harmonize())
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    unified.to_csv(OUTPUT_PATH, index=False)
    print(f"Guardado: {OUTPUT_PATH} ({unified.shape[0]} filas, {unified.shape[1]} columnas)")


if __name__ == "__main__":
    main()