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
    
    def archive_forecast(self,conn):
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO forecast_archive(id_forecast, id_station, forecast_time, vitesse_vent, rayonnement_solaire, date_archivage)
                SELECT id_forecast, id_station, forecast_time, vitesse_vent, rayonnement_solaire, CURRENT_TIMESTAMP FROM forecast;
            """)
            cur.execute("TRUNCATE TABLE forecast;")
            conn.commit()
            print("Prévisions météo archivées avec succès.")
        except Exception as e:
            conn.rollback()
            print(f"Erreur lors de l'archivage des prévisions météo : {e}")

    def save_forecast(self,df,conn):
            ## Fonction pour sauvegarder les données météo dans la bdd
            print("sauvegarde des prévisions météo...")
            isuccess = True

            sql = """
                INSERT INTO forecast (
                    id_station,
                    forecast_time,
                    vitesse_vent,
                    rayonnement_solaire
                )               
                VALUES (%s, %s, %s, %s)
            """

            try:
                cur = conn.cursor()
                #archiver les prévisions existantes avant de les écraser
                self.archive_forecast(conn)
                #cur.execute("TRUNCATE TABLE forecast;")
                records = [
                    (
                        row.id_station,
                        row.forecast_time,
                        row.vitesse_vent,
                        row.rayonnement_solaire
                    )
                    for row in df.itertuples(index=False)
                ]

                execute_batch(cur, sql, records, page_size=1000)
                conn.commit()
                print(f"{len(records)} lignes de prévisions meteo sauvegardées en base")
            except Exception as e:
                conn.rollback()
                print(f"Erreur lors de la sauvegarde des prévisions météo : {e}")
                isuccess = False

            return isuccess
