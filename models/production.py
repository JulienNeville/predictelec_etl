from psycopg2.extras import execute_batch
import pandas as pd
from db.sql_utils import python_to_sql

class Production:
    """
    Docstring for Production
    Production d'électricité par région et par filière.
    """

    def __init__(self):
        ## Constructor
        self.id=None
        self.region_id = None
        self.date_heure = None
        self.prod_heure = None
        self.prod_jour = None
        self.prod_eolien=None
        self.prod_solaire = None


    def getProductionData(self, start_date, end_date, region=None, conn=None):
        """
        Docstring for getProductionData
        
        :param start_date: date de début au format 'YYYY-MM-DD'
        :param end_date: date de fin au format 'YYYY-MM-DD'
        :param region: code de la région (optionnel)
        :param conn: connexion à la base de données ouverte
        """
        ## Fonction pour extraire les données de production depuis la BDD
        try:
            cur = conn.cursor()
            sql = """
                select *
                from production
                where (date_heure >= {start_date} and date_heure <= {end_date})
                and (region_id = {region} or {region} is null)
                order by region_id, date_heure;
            """

            data = pd.read_sql_query(
                sql.format(
                    start_date=python_to_sql(start_date),
                    end_date=python_to_sql(end_date),
                    region=python_to_sql(region)
                ), conn
            )
            
            return data
        except Exception as e:
            print(f"Erreur lors de la récupération des données de production : {e}")
            return []


    def save_lot(self, df, conn):
        ## Fonction pour sauvegarder les données de production dans la bdd
        print("sauvegarde des données de production...")
        isuccess = True

        sql = """
            INSERT INTO production (
                num_region,
                date_heure,
                prod_date,
                prod_heure,
                prod_eolien,
                prod_solaire
            )               
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (num_region, date_heure) DO UPDATE
            SET
                prod_eolien = EXCLUDED.prod_eolien,
                prod_solaire = EXCLUDED.prod_solaire
        """

        try:
            cur = conn.cursor()
            records = [
                (
                    row.code_insee_region,
                    row.date_heure,
                    row.date,
                    row.heure,
                    row.tch_eolien,
                    row.tch_solaire
                )
                for row in df.itertuples(index=False)
            ]

            execute_batch(cur, sql, records, page_size=1000)
            conn.commit()
            print(f"{len(records)} lignes de production sauvegardées en base")
        except Exception as e:
            conn.rollback()
            print(f"Erreur lors de la sauvegarde des données de production : {e}")
            isuccess = False

        return isuccess