from psycopg2.extras import execute_batch
import pandas as pd
from db.sql_utils import python_to_sql


class Meteo:
    """
    Docstring for Meteo
    """
    def __init__(self):
        self.id = None
        self.id_station = None
        self.date_validite = None
        self.meteo_date = None
        self.meteo_heure= None
        self.vitesse_vent= None
        self.rayonnement_solaire=None

    def save_lot(self,df,conn):
        ## Fonction pour sauvegarder les données météo dans la bdd
        print("sauvegarde des données météo...")
        isuccess = True

        sql = """
            INSERT INTO meteo (
                id_station,
                date_validite,
                meteo_date,
                meteo_heure,
                vitesse_vent,
                rayonnement_solaire
            )               
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_station, date_validite) DO UPDATE
            SET
                vitesse_vent = EXCLUDED.prod_eolien,
                rayonnement_solaire = EXCLUDED.prod_solaire
        """

        try:
            cur = conn.cursor()
            records = [
                (
                    row.id_station,
                    row.date_validite,
                    row.date,
                    row.heure,
                    row.vitesse_vent,
                    row.rayonnement_solaire
                )
                for row in df.itertuples(index=False)
            ]

            execute_batch(cur, sql, records, page_size=1000)
            conn.commit()
            print(f"{len(records)} lignes de meteo sauvegardées en base")
        except Exception as e:
            conn.rollback()
            print(f"Erreur lors de la sauvegarde des données météo : {e}")
            isuccess = False

        return isuccess
