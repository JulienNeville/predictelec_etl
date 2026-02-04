import json
import requests
from datetime import date,timedelta
import os
import pandas as pd
# debug : ajoute répertoire parent au PYTHONPATH
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
# fichiers __init__.py nécessaire dans chaque répertoire
from models.production import Production
from models.territoire import Territoire
from db.base import Database
from db.sql_utils import log_import
import dotenv #pip install python-dotenv

dotenv.load_dotenv()

#https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records?
# where=date_heure%20%3E=%20%222025-03-01%22%20AND%20date_heure%20%3C=%20%222025-03-03%22&limit=100

def daterange(start_date, end_date):
    """Générateur de dates (inclusives)"""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def get_save_production():
    """
    Docstring for get_save_production
    Traitement de la période + region
    """
    ###todo : gestion traitement en base ou outil externe ??
    
    # exemple avec mois complet (précédent par défaut)
    jour = date.today()

    # premier jour du mois courant
    premierjour_mois = jour.replace(day=1)

    # dernier jour du mois précédent
    dernierjour_moisprecedent = premierjour_mois - timedelta(days=1)

    # premier jour du mois précédent
    premierjour_moisprecedent = dernierjour_moisprecedent.replace(day=1)
    db = Database(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),        
        port=os.getenv('DB_PORT')
    )
    try:
        db.connect()
        liste_region = Territoire.liste_regions(db.conn)
        get_save_production_regions(premierjour_moisprecedent,dernierjour_moisprecedent,liste_region,db.conn)
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
    finally:
        db.close()

def fetch_production(region, start_datetime, end_datetime):
    """
    Docstring for fetch_production
    Appel API production par region et par date
    (attention <10000 enregistrements par limit 100)
    :param region: Description
    :param start_datetime: Description
    :param end_datetime: Description
    """
    API_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records"
    params = {
        "where": (
            f"code_insee_region = '{region}' "
            f"AND date_heure >= '{start_datetime}' "
            f"AND date_heure <= '{end_datetime}'"
        ),
        "limit": 100,
        "order_by": "date_heure"
    }

    all_rows = []
    offset = 0

    while True:
        params["offset"] = offset
        r = requests.get(API_URL, params=params)
        r.raise_for_status()
        data = r.json()["results"]

        if not data:
            break

        all_rows.extend(data)
        offset += 100

    return pd.DataFrame(all_rows)

def get_save_production_regions(start_date, end_date, regions, conn):
    """
    Import de la production par jour et par région
    """
    prod = Production()
    for day in daterange(start_date, end_date):
        day_start = f"{day}T00:00:00+00:00"
        day_end = f"{day}T23:59:59+00:00"

        for region in regions:
            try:
                print(f"Import production | Région {region} | {day}")

                # 1 appel API
                df = fetch_production(
                    region=region,
                    start_datetime=day_start,
                    end_datetime=day_end
                )

                if df.empty:
                    log_import("PROD", region, day, "SUCCESS", "Aucune donnée",conn)
                    continue

                # 2 insertion immédiate
                prod.save_lot(df, conn)

                # 3 log succès
                log_import("PROD", region, day, "SUCCESS", None,conn)

            except Exception as e:
                conn.rollback()
                log_import("PROD",region, day, "ERROR", str(e),conn)
                print(f"Erreur région {region} | {day} : {e}")

if __name__ == "__main__":
    get_save_production()

# def get_save_production_regions_old(debut_date, fin_date, region=None):

#     """
#     Récupère toutes les données eco2mix-regional-tr entre deux dates en paginant par blocs de 100.
#     - debut_date et fin_date au format 'YYYY-MM-DD'
#     - region (optionnel) : ex 'Auvergne-Rhône-Alpes'
    
#     Retourne une liste de dicts (enregistrements).
#     """

#     BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records"
#     LIMIT = 100
#     offset = 0
#     results = []

#     # prépare la clause WHERE
#     where_clause = f"date_heure >= '{debut_date}' AND date_heure <= '{fin_date}'"
#     if region:
#         where_clause += f" AND region = '{region}'"

#     while True:
#         params = {
#             "select": "code_insee_region,date_heure,date,heure,tch_eolien,tch_solaire",
#             "where": where_clause,
#             "limit": LIMIT,
#             "offset": offset,
#             "order_by": "date_heure"   # pour un ordre cohérent des données
#         }

#         response = requests.get(BASE_URL, params=params)
#         response.raise_for_status()
#         data = response.json()

#         records = data.get("results", [])
#         results.extend(records)

#         # si moins de 100 enregistrements, on a atteint la fin
#         if len(records) < LIMIT:
#             break
        
#         offset += LIMIT  # passe au bloc suivant
#         print("offset",offset)

#     # Sauvegarde des données dans la base
#     prod = Production()
#     df_records = pd.json_normalize(results) 

#     db = Database(
#         host=os.getenv('DB_HOST'),
#         dbname=os.getenv('DB_NAME'),
#         user=os.getenv('DB_USER'),
#         password=os.getenv('DB_PASSWORD'),        
#         port=os.getenv('DB_PORT')
#     )
#     try:
#         db.connect()
#         isaved = prod.save_lot(df_records, db.conn)
#         if isaved:  
#             print(f"Sauvegarde réussie de {len(results)} enregistrements de production.")
#         else:
#             print("Erreur lors de la sauvegarde des données de production.")
#     except Exception as e:
#         print(f"Erreur lors de la connexion à la base de données : {e}")
#     finally:
#         db.close()   
#     return results



# def get_save_production_regions_hier():
#     """
#     Docstring for get_save_production_regions_hier
#     Appel spécifiquement save production pour la journée d'hier
#     """
#     hier = date.now() - timedelta(days=1)
#     hier_str = hier.strftime("%Y-%m-%d")
#     return get_save_production_regions(hier_str, hier_str)

# if __name__ == "__main__":
#     ##test sur la journée d'hier
#     #all_data=get_save_production_regions_hier()
    
#     #test sur une période
#     start = "2025-07-01"
#     end   = "2025-07-07"
#     region_id = None  # ou "22"

#     all_data = get_save_production_regions(start, end, region=region_id)

#     print(f"Nombre total d'enregistrements récupérés : {len(all_data)}")
#     print(all_data[:5])  # aperçu des 5 premiers