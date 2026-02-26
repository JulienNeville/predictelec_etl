from api.api_meteo import get_valid_token
import requests
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import dotenv
import os
from db.base import Database
import pandas as pd
import time
from models.meteo import Meteo

dotenv.load_dotenv()

def meteo_header():
    '''
    Récupère un token d'authentification valide pour l'API Météo France et retourne les headers à utiliser pour les requêtes.
    '''
    # TOKEN = get_valid_token()
    TOKEN = os.getenv("TOKEN_METEO_FRANCE")
    if not TOKEN:
        raise ValueError("Pas de TOKEN METEO_FRANCE valide généré.")
    HEADERS = {"Authorization": f"Bearer {TOKEN}"}
    return HEADERS

def get_coverage_ids():
    '''
    Récupère les coverage IDs pour les paramètres de vent et de rayonnement solaire à partir de l'API GetCapabilities de Météo France.
    '''
    weather_parameters = {"vent": "Force du vent en niveaux hauteur.",
                          "rayonnement": "Flux solaire"}
    capabilities_url = "https://public-api.meteofrance.fr/public/arpege/1.0/wcs/MF-NWP-GLOBAL-ARPEGE-025-GLOBE-WCS/GetCapabilities?service=WCS&version=2.0.1&language=fre"
    response = requests.get(capabilities_url,headers=meteo_header())
    with open("capabilities.xml", "w", encoding="utf-8") as f:
        f.write(response.text)
    if response.status_code == 200:
        root = ET.fromstring(response.text)
        ns = {
            "wcs": "http://www.opengis.net/wcs/2.0",
            "ows": "http://www.opengis.net/ows/2.0",
            }
        coverage_ids = {}
        # récupérer tous les CoverageSummary avec ce Title
        for param, title in weather_parameters.items():
            ids = []
            for cs in root.findall(".//wcs:CoverageSummary", ns):
                title = cs.findtext("ows:Title", default="", namespaces=ns)
                if title == weather_parameters[param]:
                    cov_id = cs.findtext("wcs:CoverageId", default="", namespaces=ns)
                    if cov_id:
                        if param == "vent":
                            ids.append(cov_id)
                        elif param == "rayonnement" and cov_id.endswith("PT1H"):
                            ids.append(cov_id)
            if ids:
                last_id = ids[-1]   # dernier dans l'ordre du XML
                coverage_ids[param] = last_id
            else:
                print(f"Aucun CoverageId trouvé pour le paramètre de titre'{param}'")
        return coverage_ids
    else:
        print(response.status_code)
        return None

def get_time_steps(coverage_id):
    '''
    Récupère les pas de temps disponibles pour un coverage ID donné à partir de l'API DescribeCoverage de Météo France.
    '''
    describe_coverage_url = f"https://public-api.meteofrance.fr/public/arpege/1.0/wcs/MF-NWP-GLOBAL-ARPEGE-025-GLOBE-WCS/DescribeCoverage?service=WCS&version=2.0.1&coverageID={coverage_id}"
    response = requests.get(describe_coverage_url, headers=meteo_header())
    if response.status_code == 200:
        root = ET.fromstring(response.text)
        ns = {
            "gml": "http://www.opengis.net/gml/3.2",
            "gmlrgrid": "http://www.opengis.net/gml/3.3/rgrid",
        }
        # 1) Date de début (ISO-8601)
        begin_text = root.findtext(".//gml:beginPosition", namespaces=ns).strip()
        t0 = datetime.fromisoformat(begin_text.replace("Z", "+00:00"))

        # 2) Coefficients temporels (en secondes depuis begin)
        coeff_elem = root.find(".//gmlrgrid:GeneralGridAxis[gmlrgrid:gridAxesSpanned='time']/gmlrgrid:coefficients", ns)
        coeffs = [int(v) for v in coeff_elem.text.split()]

        # 3) Générer la liste de dates ISO-8601
        dates_iso = [(t0 + timedelta(seconds=sec)).isoformat().replace("+00:00", "Z")
                    for sec in coeffs]
        return dates_iso
    else:
        print(f"Failed to retrieve time steps for {coverage_id}")
        return None

def update_forecast_db():
    '''
    Met à jour la base de données en:
    - supprimant les prévisions du jour (anciennes J+1);
    - ajoutant les prévisions à J+5 (nouvelles J+4);
    - mettant à jour les prévisions à J+2 (nouvelles J+1), J+3 (nouvelles J+2) et J+4 (nouvelles J+3).
    '''
