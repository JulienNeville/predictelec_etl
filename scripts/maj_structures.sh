#!/bin/bash

# autorise erreur sans stopper le script : pas -e, sinon on ne récupère pas l'erreur à envoyer par mail
set +e

# Empêche plusieurs exécutions en même temps
LOCK_FILE="/tmp/maj_structures.lock"

if [ -f "$LOCK_FILE" ]; then
    echo "Script déjà en cours d'exécution." >> "$LOG_FILE"
    exit 1
fi

touch "$LOCK_FILE"
# tue le fichier lock en cas d'interruption (pas de blocage à cause du fichier lock présent alors que rien ne tourne)
trap "rm -f $LOCK_FILE" EXIT


# Aller dans le dossier parent du dossier du script
cd "$(dirname "$0")/.."

PROJECT_DIR="$(pwd)"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/maj_structures.log"

mkdir -p "$LOG_DIR"

echo "----------------------------------------" >> "$LOG_FILE"
echo "Début MAJ_STRUCTURES : $(date)" >> "$LOG_FILE"

# Exécution du job 
# ajout --no-deps pour ne pas relancer postgres
docker compose run --rm --no-deps app MAJ_STRUCTURES >> "$LOG_FILE" 2>&1

#stop le script si erreur
set -e

if [ $EXIT_CODE -ne 0 ]; then
	#on log l'erreur
	echo "Erreur MAJ_STRUCTURES le $(date)" >> "$LOG_FILE"
	#envoie de l'erreur
	echo "Erreur MAJ_STRUCTURES le $(date)" | mail -s "ERREUR PREDICTELEC MAJ_STRUCTURES" famille.nony@sfr.fr
else
	echo "Fin MAJ_STRUCTURES : $(date)" >> "$LOG_FILE"
fi

echo "" >> "$LOG_FILE"

#quitte avec le code erreur
exit $EXIT_CODE
