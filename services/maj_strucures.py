from services.maj_installations import get_save_allinstallations as maj_installations
from services.maj_installations import save_installations_geoloc as maj_geoloc
from services.maj_stations import get_save_stations_eligibles as maj_stations
from services.combine_installations_stations import combine_installations_stations_eligibles as combine

print("Mise à jour mensuelle des installations et des stations...")
maj_installations()
maj_geoloc()
maj_stations()
combine()