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
        self.validity_time = None
        self.vitesse_vent= None
        self.rayonnement_solaire=None

    def save_lot(self,df,conn):
        ## Fonction pour sauvegarder les données météo dans la bdd
        print("sauvegarde des données météo...")
        isuccess = True

        sql = """
            INSERT INTO meteo (
                id_station,
                validity_time,
                vitesse_vent,
                rayonnement_solaire
            )               
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_station, validity_time) DO UPDATE
                SET
                    vitesse_vent = EXCLUDED.vitesse_vent,
                    rayonnement_solaire = EXCLUDED.rayonnement_solaire
        """

        try:
            cur = conn.cursor()
            records = [
                (
                    row.id_station,
                    row.validity_time,
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
