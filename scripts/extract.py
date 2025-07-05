import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from datetime import datetime
from pathlib import Path

# Configuration
RAW_DIR = Path("data/raw")
API_KEY = "...." 
CITIES = ["Paris", "London", "New York", "Tokyo", "Berlin", "Sydney", "Moscow", "Dubai","Antananarivo"]

def fetch_weather(city):
    """Récupère les données météo pour une ville"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    
    return {
        'city': city,
        'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'temp': data['main']['temp'],
        'temp_min': data['main']['temp_min'],
        'temp_max': data['main']['temp_max'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'wind_speed': data['wind']['speed'],
        'weather': data['weather'][0]['main'],
        'rain': data.get('rain', {}).get('1h', 0)
    }

def save_raw_data():
    """Sauvegarde les données brutes par date"""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    all_data = []
    for city in CITIES:
        try:
            all_data.append(fetch_weather(city))
        except Exception as e:
            print(f"Erreur pour {city}: {str(e)}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        date_str = datetime.now().strftime("%Y%m%d")
        filename = RAW_DIR / f"weather_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"Données sauvegardées dans {filename}")
        
        # Nettoyage des vieux fichiers (conserver 7 jours)
        clean_old_files(keep_days=7)
        
        return filename
    return None

def clean_old_files(keep_days=7):
    """Supprime les fichiers de plus de keep_days jours"""
    cutoff = datetime.now() - timedelta(days=keep_days)
    for file in RAW_DIR.glob("weather_*.csv"):
        date_str = file.stem.split("_")[1]
        file_date = datetime.strptime(date_str, "%Y%m%d")
        if file_date < cutoff:
            file.unlink()
            print(f"Supprimé {file.name}")

if __name__ == "__main__":
    save_raw_data()