### structure sur la VM
/opt/predictelec/
│
├── releases/
│   ├── 2026-02-17/
│   └── 2026-02-25/
│
├── shared/
│   ├── .env
│   └── logs/
│
└── current/predictelec -> releases/2026-02-17

Note: namespace "predictelec" (current/predictelec) ajouté pour que Airflow trouve le projet et le lie à son plugins

###déploiement d'une nouvelle version
##Etape 1 : créer une release
# se positionner sur le répertoire releases
cd /opt/predictelec/releases

# cloner le projet depuis git hub et la branche souhaitée vers répertoire nom=date du jour (=version)
git clone --branch test-collegue --depth 1 https://github.com/JulienNeville/predictelec_etl.git 2026-03-26

##Etape 2 : lier le .env partagé (dans shared)
# créer ou mettre à jour le .env du répertoire /opt/predictelec/shared (voir le cas échéant les paramètres proposés dans .env.prod)

# créer un lien symlink avec la release de prod
ln -s /opt/predictelec/shared/.env /opt/predictelec/releases/2026-03-26/.env

##Etape 3 : pointer le predictelec current avec la release de prod
# le dossier namespace predictelec ne doit pas exister dans /opt/predictelec/current, faire un rm si besoin
#S'il existe le lien créera un dossier du nom de la release, s'il n'existe pas la cmd créera le dossier predictelec et le liera avec le dossier release demandé
rm /opt/predictelec/current/predictelec

# créer un lien symlink entre la release de prod et le dossier current
ln -sfn /opt/predictelec/releases/2026-03-26 /opt/predictelec/current/predictelec

# vérifier
ls -l /opt/predictelec/current/

# on doit voir à quelle release le namespace predictelec est lié
predictelec -> /opt/predictelec/releases/2026-03-26

###lancement du docker du projet predictelec
#depuis le dossier /opt/predictelec/current/predictelec
cd /opt/predictelec/current/predictelec
docker compose up -d --build
#attention pour prise en compte par le docker airflow nécessite de redémarrer le airflow-scheduler et les airflow-workers
cd /root/airflow
docker compose restart airflow-scheduler airflow-worker

###initialiser la base de données
## A ne lancer que la 1ère fois
#création de la bdd, des tables et des vues
docker compose run app INIT

## A lancer au besoin
#si update des vues ou création de nouvelles vues
docker compose run app INIT_VIEWS

#pour rafraîchir les données des vues matérialisées
docker compose run app REFRESH_VIEWS

## A lancer manuellement pour recette avant orchestration dans Airflow
#LANCEMENT MAJ_STRUCTURES : centrales électriques + stations météos + liens centrales-stations
docker compose run app MAJ_STRUCTURES

#LANCEMENT MAJ_PROD + refresh_views : données de production électrique
docker compose run app MAJ_PROD

#LANCEMENT MAJ_METEO + refresh_views : données météo récentes
docker compose run app MAJ_METEO

#LANCEMENT MAJ_METEO_PREC + refresh_views : données météo historiques depuis mois précédent
docker compose run app MAJ_METEO_PREC

#LANCEMENT MAJ_METEO_PREC_MOIS + refresh_views : données météo historiques depuis mois donnée
docker compose run app MAJ_METEO_PREC_MOIS 2026-01-01

### Orchestration Airflow : mise en place
##Architecture
#créer un dossier airflow
mkdir airflow
#se positionner dans le dossier
cd airflow
#créer les dossiers dags, config, logs, plugins et leur donner les droits 
mkdir -p dags logs plugins config
chmod -R 777 dags logs plugins config
#ou pour seulement airflow
sudo chown -R 50000:0 logs dags plugins

### Paramètrage Airflow
#créer ou placer le fichier docker-compose.yml directement dans dossier airflow
#créer un fichier .env directement dans dossier airflow

##paramètrage dans docker-compose.yml
# utilisation .env par airflow dans YAML
env_file:
  - .env
# utilisation CeleryExecutor + redis permettant la distribution des tâches en parallèle
environnement:
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor
  AIRFLOW__CELERY__BROKEN_URL: redis://redis:6379/0
  # paramétrage 10 tâches en parallèle dont 5 par worker (2 workers max) sans aspiration auto de tâches (hors prévu dans le code du DAG)
  AIRFLOW__CORE__PARALLELISM: 10
  AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 10
  AIRFLOW__CELERY__WORKER_CONCURRENCY: 5
  AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER: 1
# utilisation du code source du projet predictelec : chargement plugins
environnement:
  PYTHONPATH: /opt/predictelec/current/predictelec:/opt/airflow/plugins 
# utilisation base postgres spécifique pour airflow et celery
environnement:
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
  AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
# volumes persistants pour airflow
volumes:
  - ./dags:/opt/airflow/dags  
  - ./logs:/opt/airflow/logs
  - ./plugins:/plugins/airflow/plugins
  - /opt/predictelec:/opt/predictelec
# volume persistant pour la base postgres airflow
services:
  postgres:
    volumes:
      - postgres-db-airflow-volume:/var/lib/postgresql/data
volumes:
  postgres-db-airflow-volume:
# services à paramètrer :
services:
  postgres
  redis
  airflow-webserver
  airflow-scheduler
  airflow-worker
  airflow-init

### Mise en service Airflow  
#lancer le docker
docker compose up --build
#si relance
docker compose up -d --build
#si base posgres pour airflow non démarré
docker compose up airflow-init
#pour lancer le nombre de 2 workers (qui effectueront 5 tâches chacuns)
docker compose up --scale airflow-worker=2

### Accès interface Airflow
IP_VM::8080
username+password paramétré dans airflow-init
