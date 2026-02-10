import argparse
from services.init_base import init
from services.maj_installations import get_save_allinstallations as maj_installations
from services.maj_installations import save_installations_geoloc as maj_geoloc
from services.maj_stations import get_save_stations_eligibles as maj_stations
from services.combine_installations_stations import combine_installations_stations_eligibles as combine
from services.maj_meteo import get_save_meteo_hier as maj_meteo_quotidien
from services.maj_meteo import import_meteo_previous_month as maj_meteo_mois_precedent
from services.maj_production import get_save_production as maj_production_mois_precedent

def main(action=None):
    print(f"Action donnée : {action}")
    #MODE CLI
    if action is None:
        parser = argparse.ArgumentParser(description="Gestion des opérations Predictelec")
        parser.add_argument("action", choices=["INIT", "MAJ_STRUCTURES", "MAJ_PROD", "MAJ_METEO","MAJ_METEO_PREC", "MAJ_PREVISION"])

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
        maj_production_mois_precedent()

    elif action == "MAJ_METEO":
        print("Mise à jour journalière des données météorologiques...")
        maj_meteo_quotidien()

    elif action == "MAJ_METEO_PREC":
        print("Mise à jour mensuelle des données météorologiques...")
        maj_meteo_mois_precedent()

if __name__ == "__main__":
    #debug
    #main("INIT")
    #main("MAJ_STRUCTURES")
    #main("MAJ_PROD")
    #main("MAJ_METEO")
    #main("MAJ_METEO_PREC")
    
    #prod
    main()
