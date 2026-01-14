import argparse
from services.init_base import init
from services.maj_installations import get_save_allinstallations as maj_installations
from services.maj_installations import save_installations_geoloc as maj_geoloc
from services.maj_stations import get_save_stations_eligibles as maj_stations
from services.combine_installations_stations import combine_installations_stations_eligibles as combine
from services.maj_meteo import get_save_meteo as maj_meteo
from services.maj_production import get_save_production as maj_production
from services.maj_prevision import get_save_prevision as maj_prevision
import dotenv #pip install python-dotenv 

def main(action=None):
    #MODE CLI
    if action is None:
        parser = argparse.ArgumentParser(description="Gestion des opérations Predictelec")
        parser.add_argument("action", choices=["INIT", "MAJ_STRUCTURES", "MAJ_PROD", "MAJ_METEO", "MAJ_PREVISION"])

        args = parser.parse_args()
        action=args.action



    # MODE APPEL INTERNE / DEBUG : utilise action passée en paramètre
    
    print(f"Action exécutée : {action}")

    if action == "INIT":
        print("Initialisation base de données...")
        init()

    elif action == "MAJ_STRUCTURES":
        print("Mise à jour mensuelle des installations et des stations...")
        maj_installations()
        maj_geoloc()
        maj_stations()
        combine()
        

    elif action == "MAJ_PROD":
        print("Mise à jour journalière des données de production...")
        maj_production()

    elif action == "MAJ_METEO":
        print("Mise à jour journalière des données météorologiques...")
        maj_meteo()

    elif action == "MAJ_PREVISION":
        print("Mise à jour journalière des prévisions météorologiques...")
        maj_prevision()



if __name__ == "__main__":
    ##test INIT --> ok
    #main("INIT")

    ##test MAJ_STRUCTURES --> ok
    #main("MAJ_STRUCTURES")

    ##test MAJ_PROD --> ok
    #main("MAJ_PROD")

    ##test MAJ_METEO --> quel API ? quels paramètres ?
    main("MAJ_METEO")

    ##test MAJ_PREVISION --> quel API ? quels paramètres ?
    #main("MAJ_PREVISION")    