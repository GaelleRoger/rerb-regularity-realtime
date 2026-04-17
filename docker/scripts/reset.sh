#!/bin/bash
# Reset quotidien — exporte les tables puis réinitialise Postgres
# Exécuté à 1h01 chaque nuit
# Logs : /app/logs/reset_YYYY-MM-DD.log (rotation journalière, 7 jours conservés)

LOG_DIR=/app/logs
LOG_FILE="$LOG_DIR/reset_$(date '+%Y-%m-%d').log"
SEP="========================================"

# Rotation : suppression des logs de plus de 7 jours
find "$LOG_DIR" -name "reset_*.log" -mtime +7 -delete

echo "$SEP"                                         >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Début reset"  >> "$LOG_FILE"

cd /app/src/pipeline

python3 reset_postgres.py >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%H:%M:%S')] Reset terminé avec succès" >> "$LOG_FILE"
else
    echo "[$(date '+%H:%M:%S')] Reset ÉCHOUÉ (code $EXIT_CODE)" >> "$LOG_FILE"
fi
