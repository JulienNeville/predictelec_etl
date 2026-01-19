import sys
from pathlib import Path

# Ajouter le répertoire parent au chemin Python
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import base
from models.territoire import Territoire
import os
import dotenv #pip install python-dotenv

dotenv.load_dotenv()

db = base.Database(
host=os.getenv('DB_HOST'),
dbname=os.getenv('DB_NAME'),
user=os.getenv('DB_USER'),
password=os.getenv('DB_PASSWORD'),        
port=os.getenv('DB_PORT')
)
# Créer la base de données
db.create_base()
# Créer les tables nécessaires
db.create_tables()
# Connection
db.connect()
# Importe départements + regions
db.connect()
Territoire.init_dep_region(db.conn)
# Fermer la connexion à la base de données
db.close()