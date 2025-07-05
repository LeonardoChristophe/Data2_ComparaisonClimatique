# Configuration de l'API OpenWeather
OPENWEATHER_API_KEY = "...."  
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"


# Villes à suivre
CITIES = [
    "Paris", "London", "New York", "Tokyo", 
    "Berlin", "Sydney", "Moscow", "Dubai"
]

DATE_RANGE = {
    "start": "2023-01-01",
    "end": "2023-12-31"
}


# Configuration des chemins
RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"
STAR_SCHEMA_PATH = "data/star_schema"

