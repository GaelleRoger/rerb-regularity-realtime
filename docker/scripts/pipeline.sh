#!/bin/bash
# Pipeline ETL — extract → load → référentiel → calculs
# Exécuté toutes les 5 minutes (sauf entre 1h et 3h, fenêtre de reset)
# Logs : /app/logs/pipeline_YYYY-MM-DD.log (rotation journalière, 7 jours conservés)

LOG_DIR=/app/logs
LOG_FILE="$LOG_DIR/pipeline_$(date '+%Y-%m-%d').log"
SEP="========================================"

# Rotation : suppression des logs de plus de 7 jours
find "$LOG_DIR" -name "pipeline_*.log" -mtime +7 -delete

echo "$SEP"                                           >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Début pipeline" >> "$LOG_FILE"

cd /app/src/pipeline

run() {
    echo "[$(date '+%H:%M:%S')] → $1" >> "$LOG_FILE"
    python3 "$1" >> "$LOG_FILE" 2>&1
    return $?
}

run extract_horaires.py       && \
run load_postgres.py          && \
run creation_table_ref.py     && \
run calcul_regularite.py      && \
run calcul_ecarts_horaires.py && \
run calcul_allongement_parcours.py

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%H:%M:%S')] Pipeline terminé avec succès" >> "$LOG_FILE"
else
    echo "[$(date '+%H:%M:%S')] Pipeline ÉCHOUÉ (code $EXIT_CODE)" >> "$LOG_FILE"
fi
