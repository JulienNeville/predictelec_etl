import sys
import os
from pathlib import Path
import time

# Ajouter le répertoire parent au chemin Python
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import pandas as pd
from models.meteo import Meteo
from models.station import Station
from db.base import Database
import dotenv
from api.api_meteo import get_valid_token
from datetime import datetime, timedelta

dotenv.load_dotenv()

# Sauvegarde des données dans la base
def save_data(df_stations):
    meteo = Meteo()
    db = Database(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),        
        port=os.getenv('DB_PORT')
    )
    try:
        db.connect()
        isaved = meteo.save_lot(df_stations, db.conn)
        if isaved:  
            print(f"Sauvegarde réussie de {df_stations.shape[0]} enregistrements de production.")
        else:
            print("Erreur lors de la sauvegarde des données de production.")
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
    finally:
        db.close()   

    return df_stations

# def get_save_meteo_past_month():
#     # L'API retourne une liste vide
#     """
#     Docstring pour get_save_meteo_past_month
#     """
#     TOKEN = get_valid_token()
#     if not TOKEN:
#         raise ValueError("Pas de TOKEN METEO_FRANCE valide généré.")
#     HEADERS = {"Authorization": f"Bearer {TOKEN}"}

#     df_stations_data = pd.DataFrame()
#     df_stations_data_select = pd.DataFrame()

#     # Calcul des dates de début et de fin du mois précédent
#     today = datetime.today()
#     first_day_of_current_month = today.replace(day=1)
#     last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
#     first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
#     last_day_of_previous_month_str = last_day_of_previous_month.strftime("%Y-%m-%dT%H:%M:00Z")
#     first_day_of_previous_month_str = first_day_of_previous_month.strftime("%Y-%m-%dT00:06:00Z")

#     BASE_URL = f"https://public-api.meteofrance.fr/public/DPPaquetObs/v1/paquet/stations/infrahoraire-6m?date={first_day_of_previous_month_str}&format=json"
#     try:
#         response = requests.get(BASE_URL, headers=HEADERS)
#         response.raise_for_status()
#         data = response.json()
#         df_data = pd.json_normalize(data)
#         df_stations_data = pd.concat([df_stations_data, df_data], ignore_index=True)
#         print(data)
#         # df_stations_data_select = df_stations_data[["geo_id_insee","validity_time","ff","ray_glo01"]].copy()
#         # df_stations_data_select.rename(columns={"geo_id_insee" : "id_station","validity_time" : "date_validite","ff":"vitesse_vent","ray_glo01":"rayonnement_solaire"},inplace=True)
        
#     except Exception as e:
#         print(f"Erreur lors de la requête : {e}")

#     finally:
#         save_data(df_stations_data_select)

def get_save_meteo_hier():

    """
    Récupère toutes les données meteo entre deux dates en paginant par blocs de 100.
    - debut_date et fin_date au format 'YYYY-MM-DD'
    - region (optionnel) : ex 'Auvergne-Rhône-Alpes'
    
    Retourne une liste de dicts (enregistrements).
    """
    df_stations_data = pd.DataFrame()
    df_stations_data_select = pd.DataFrame()
    db = Database(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),        
        port=os.getenv('DB_PORT')
    )
    station = Station()

    TOKEN = get_valid_token()
    if not TOKEN:
        raise ValueError("Pas de TOKEN METEO_FRANCE valide généré.")
    HEADERS = {"Authorization": f"Bearer {TOKEN}"}

    try:
        db.connect()
        stations = station.getlistStation(db.conn)
        liste_stations = stations['id_station']
        print(len(liste_stations), "stations météo à traiter.")
        twentyth = len(liste_stations) // 20
        liste_stations1 = liste_stations[:twentyth]
        liste_stations2 = liste_stations[twentyth:2*twentyth]
        liste_stations3 = liste_stations[2*twentyth:3*twentyth]
        liste_stations4 = liste_stations[3*twentyth:4*twentyth]
        liste_stations5 = liste_stations[4*twentyth:5*twentyth]
        liste_stations6 = liste_stations[5*twentyth:6*twentyth]
        liste_stations7 = liste_stations[6*twentyth:7*twentyth]
        liste_stations8 = liste_stations[7*twentyth:8*twentyth]
        liste_stations9 = liste_stations[8*twentyth:9*twentyth]
        liste_stations10 = liste_stations[9*twentyth:10*twentyth]
        liste_stations11 = liste_stations[10*twentyth:11*twentyth]
        liste_stations12 = liste_stations[11*twentyth:12*twentyth]
        liste_stations13 = liste_stations[12*twentyth:13*twentyth]
        liste_stations14 = liste_stations[13*twentyth:14*twentyth]
        liste_stations15 = liste_stations[14*twentyth:15*twentyth]
        liste_stations16 = liste_stations[15*twentyth:16*twentyth]
        liste_stations17 = liste_stations[16*twentyth:17*twentyth]
        liste_stations18 = liste_stations[17*twentyth:18*twentyth]
        liste_stations19 = liste_stations[18*twentyth:19*twentyth]
        liste_stations20 = liste_stations[19*twentyth:]
        liste_listes_stations = [liste_stations1, liste_stations2, liste_stations3, liste_stations4, liste_stations5, liste_stations6, liste_stations7, liste_stations8, liste_stations9, liste_stations10, liste_stations11, liste_stations12, liste_stations13, liste_stations14, liste_stations15, liste_stations16, liste_stations17, liste_stations18, liste_stations19, liste_stations20]
        for liste_stations in liste_listes_stations:
            for station in liste_stations:
                station_str = str(station)
                if len(station_str)<8:
                    station_str = station_str.zfill(8)
                BASE_URL = f"https://public-api.meteofrance.fr/public/DPPaquetObs/v1/paquet/infrahoraire-6m?id_station={station_str}&format=json"
                response = requests.get(BASE_URL, headers=HEADERS)
                response.raise_for_status()
                data = response.json()
                df_data = pd.json_normalize(data)
                df_stations_data = pd.concat([df_stations_data, df_data], ignore_index=True)
                print(f"Station n° {station} traitée.")
            time.sleep(12)
        df_stations_data_select = df_stations_data[["geo_id_insee","validity_time","ff","ray_glo01"]].copy()
        df_stations_data_select.rename(columns={"geo_id_insee" : "id_station","validity_time" : "date_validite","ff":"vitesse_vent","ray_glo01":"rayonnement_solaire"},inplace=True)
       
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")

    finally:
        save_data(df_stations_data_select)

get_save_meteo_hier()