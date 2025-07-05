import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
import json


# Configuration
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
BACKUP_DIR = PROCESSED_DIR / "backup"

def merge_files():
    """Fusionne tous les fichiers bruts en un seul fichier traité"""
    # Création des répertoires
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True)
    
    # Ajoutez en début de script
def merge_all_data():
    """Fusionne données actuelles ET historiques"""
    # Fusion des données actuelles (existant)
    df_current = pd.concat([pd.read_csv(f) for f in Path("data/raw").glob("weather_*.csv")])
    
    # Fusion des données historiques
    df_historical = pd.concat([pd.read_csv(f) for f in Path("data/historical/processed").glob("*.csv")])
    
    # Concaténation finale
    return pd.concat([df_current, df_historical])

    
    # Sauvegarde de l'ancien fichier fusionné
    merged_file = PROCESSED_DIR / "merged_weather.csv"
    if merged_file.exists():
        backup_file = BACKUP_DIR / f"merged_weather_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        shutil.copy2(merged_file, backup_file)
    
    # Lecture et fusion des fichiers bruts
    all_files = list(RAW_DIR.glob("weather_*.csv"))
    if not all_files:
        raise FileNotFoundError("Aucun fichier brut trouvé")
    
    dfs = []
    for file in sorted(all_files):
        df = pd.read_csv(file)
        dfs.append(df)
    
    merged_df = pd.concat(dfs, ignore_index=True)

    
    
    # Nettoyage des données
    merged_df['date'] = pd.to_datetime(merged_df['date'])
    merged_df.drop_duplicates(subset=['city', 'date'], keep='last', inplace=True)
    merged_df.sort_values(['city', 'date'], inplace=True)
    
    # Sauvegarde
    merged_df.to_csv(merged_file, index=False)
    print(f"Données fusionnées sauvegardées dans {merged_file}")
    
    # Nettoyage des sauvegardes (garder les 5 dernières)
    clean_backups(keep=5)

    
    
    return merged_file

def clean_backups(keep=5):
    """Garde seulement les 'keep' dernières sauvegardes"""
    backups = sorted(BACKUP_DIR.glob("merged_weather_*.csv"), reverse=True)
    for old_backup in backups[keep:]:
        old_backup.unlink()
        print(f"Supprimé la sauvegarde {old_backup.name}")
        ##
def generate_stats_json(df):
    """Génère un fichier JSON avec les statistiques météo par ville"""
    try:
        # Calcul des statistiques
        stats = df.groupby('city').agg({
            'temp': ['mean', 'max', 'min'],
            'rain': 'sum',
            'humidity': 'mean'
        }).round(2).to_dict()
        
        # Ajout de métadonnées
        stats_metadata = {
            "generated_at": datetime.now().isoformat(),
            "cities_count": len(df['city'].unique()),
            "date_range": {
                "start": df['date'].min().strftime('%Y-%m-%d'),
                "end": df['date'].max().strftime('%Y-%m-%d')
            },
            "statistics": stats
        }
        
        # Sauvegarde du JSON
        stats_file = PROCESSED_DIR / "weather_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Statistiques JSON sauvegardées dans {stats_file}")
        return stats_file
    except Exception as e:
        print(f"Erreur lors de la génération des stats JSON: {str(e)}")
        return None

if __name__ == "__main__":
    merge_files()