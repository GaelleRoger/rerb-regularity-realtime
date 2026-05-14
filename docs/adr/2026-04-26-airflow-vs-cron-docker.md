# ADR-001 : Orchestration — Airflow remplacé par cron Docker

## Statut
✅ Accepté

## Contexte

Le pipeline ETL du projet enchaîne six scripts Python dans un ordre fixe, toutes les 5 minutes, avec un reset quotidien à 1h01. Une première version du projet utilisait Apache Airflow comme orchestrateur.

Airflow introduisait les contraintes suivantes dans ce contexte :
- **Surcharge opérationnelle** : scheduler, webserver, worker et base de métadonnées dédiée (4 services supplémentaires) pour orchestrer 6 scripts séquentiels
- **Consommation mémoire** : Airflow nécessite ~1–2 Go de RAM au repos, incompatible avec une VM de démonstration à faible coût
- **Complexité de configuration** : DAGs, connexions, variables Airflow — autant de couches d'abstraction inutiles pour un pipeline sans branchement conditionnel
- **Temps de démarrage** : l'initialisation d'Airflow (`airflow db init`, `airflow users create`) alourdit significativement le `docker compose up`

Le pipeline ne présente aucune caractéristique justifiant Airflow : pas de dépendances conditionnelles entre tâches, pas de parallélisme, pas de backfill, pas de gestion de SLA complexe.

## Décision

Le pipeline est orchestré par **deux tâches cron** dans un conteneur Docker dédié (`python:3.12-slim` + `cron`) :

- `*/5 0,3-23 * * *` → exécute les 6 scripts en séquence via `pipeline.sh`
- `1 1 * * *` → reset quotidien de la base via `reset.sh`

Les logs sont rotatifs (7 jours), écrits dans `logs/pipeline_YYYY-MM-DD.log`.

## Alternatives considérées

| Alternative | Raison du rejet |
| ----------- | --------------- |
| **Apache Airflow** | Surcharge opérationnelle disproportionnée pour 6 scripts séquentiels sur schedule fixe |
| **Prefect / Dagster** | Même constat : valeur ajoutée nulle pour un pipeline sans branchement ni dépendance dynamique |
| **Kubernetes CronJob** | Infrastructure trop lourde pour un projet mono-machine |

## Conséquences

**Avantages :**
- Stack réduite de 4 services à 1 (conteneur `rerb-cron`)
- RAM libérée (~1,5 Go) disponible pour Postgres, l'API et Grafana
- `docker compose up` fonctionnel en moins de 30 secondes
- Lisibilité maximale : `pipeline.sh` est un script bash de 35 lignes

**Inconvénients et risques assumés :**
- Pas d'interface de monitoring des exécutions (remplacé par les logs fichiers + Grafana pour la disponibilité de l'API)
- Pas de retry automatique en cas d'échec partiel — une exécution ratée attend le prochain cycle (5 min)
- Migration vers Airflow/Prefect envisageable si le pipeline gagne en complexité (branchements, sources multiples, SLA stricts)
