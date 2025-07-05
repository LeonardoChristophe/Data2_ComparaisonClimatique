import pandas as pd
from pathlib import Path
from datetime import datetime

# Configuration
PROCESSED_FILE = Path("data/processed/merged_weather.csv")
STAR_DIR = Path("data/star_schema")

def create_star_schema():
    """Crée le modèle en étoile"""
    STAR_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lecture des données fusionnées
    df = pd.read_csv(PROCESSED_FILE)
    df['date'] = pd.to_datetime(df['date'])
    
    # Table de faits
    fact_cols = ['date', 'city', 'temp', 'temp_min', 'temp_max', 
                'humidity', 'pressure', 'wind_speed', 'rain']
    fact_df = df[fact_cols]
    fact_df.to_csv(STAR_DIR / "fact_weather.csv", index=False)
    
    # Dimension Ville
    city_dim = df[['city']].drop_duplicates()
    city_dim['city_id'] = range(1, len(city_dim)+1)
    city_dim.to_csv(STAR_DIR / "dim_city.csv", index=False)
    
    # Dimension Date
    date_dim = df[['date']].drop_duplicates()
    date_dim['date_id'] = range(1, len(date_dim)+1)
    date_dim['day'] = date_dim['date'].dt.day
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['day_of_week'] = date_dim['date'].dt.dayofweek
    date_dim.to_csv(STAR_DIR / "dim_date.csv", index=False)
    
    # Création des clés étrangères dans la table de faits
    fact_with_keys = fact_df.merge(
        city_dim[['city', 'city_id']], 
        on='city'
    ).merge(
        date_dim[['date', 'date_id']],
        on='date'
    )
    
    # Sauvegarde finale
    fact_with_keys.to_csv(STAR_DIR / "fact_weather_with_keys.csv", index=False)
    
    print("Modèle en étoile généré avec succès")
    return {
        'fact_table': STAR_DIR / "fact_weather_with_keys.csv",
        'dim_city': STAR_DIR / "dim_city.csv",
        'dim_date': STAR_DIR / "dim_date.csv"
    }

if __name__ == "__main__":
    create_star_schema()