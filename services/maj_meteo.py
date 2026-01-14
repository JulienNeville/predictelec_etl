import requests
from datetime import datetime, timedelta
import os
import pandas as pd

from models.meteo import Meteo
from db.base import Database
import dotenv #pip install python-dotenv

dotenv.load_dotenv()


def get_save_meteo(debut_date, fin_date, region=None):

    """
    Récupère toutes les données metteeo entre deux dates en paginant par blocs de 100.
    - debut_date et fin_date au format 'YYYY-MM-DD'
    - region (optionnel) : ex 'Auvergne-Rhône-Alpes'
    
    Retourne une liste de dicts (enregistrements).
    """

    ###todo: trouver la bonne  url et les paramètres correspondant
    BASE_URL = "https://public-api.meteofrance.fr/public/DPPaquetObs/v1/paquet/infrahoraire-6m"
    LIMIT = 100
    offset = 0
    results = []

    # prépare la clause WHERE
    where_clause = f"date_heure >= '{debut_date}' AND date_heure <= '{fin_date}'"
    if region:
        where_clause += f" AND region = '{region}'"

    ###toddo: voir les paramètres à envoyer (syntaxe)
    while True:
        params = {
            "xxx1": "xxx",
            "xxx2": "xxx"  ,
            "where": where_clause,
            "limit": LIMIT,
            "offset": offset
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        records = data.get("results", [])
        results.extend(records)

        # si moins de 100 enregistrements, on a atteint la fin
        if len(records) < LIMIT:
            break
        
        offset += LIMIT  # passe au bloc suivant
        print("offset",offset)

    # Sauvegarde des données dans la base
    meteo = Meteo()

    ###todo: vérifier correspondance colonnes avec l'attendu (vent+soleil)
    df_records = pd.json_normalize(results) 

    db = Database(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),        
        port=os.getenv('DB_PORT')
    )
    try:
        db.connect()
        isaved = meteo.save_lot(df_records, db.conn)
        if isaved:  
            print(f"Sauvegarde réussie de {len(results)} enregistrements de production.")
        else:
            print("Erreur lors de la sauvegarde des données de production.")
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
    finally:
        db.close()   
    return results




def get_save_meteo_hier():
    """
    Docstring for get_save_meteo_hier
    Appel spécifiquement save meteo pour la journée d'hier
    """
    hier = datetime.now() - timedelta(days=1)
    hier_str = hier.strftime("%Y-%m-%d")
    return get_save_meteo(hier_str, hier_str)

if __name__ == "__main__":
    ##test sur la journée d'hier
    #all_data=get_save_meteo_hier()
    
    #test sur une période
    start = "2025-07-01"
    end   = "2025-07-07"
    region_id = None  # ou "22"

    all_data = get_save_meteo(start, end, region=region_id)

    print(f"Nombre total d'enregistrements récupérés : {len(all_data)}")
    print(all_data[:5])  # aperçu des 5 premiers