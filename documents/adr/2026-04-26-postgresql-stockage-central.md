# ADR-002 : Stockage — PostgreSQL comme base de données centrale

## Statut
✅ Accepté

## Contexte

Le pipeline produit, toutes les 5 minutes, trois snapshots agrégés (régularité, allongement, écarts horaires) représentant environ 6 lignes par cycle. Les tables de référence (horaires théoriques, missions en circulation) contiennent quelques milliers de lignes, reconstruites intégralement à chaque exécution.

Caractéristiques du workload :
- **Volume faible** : ~1 700 écritures/jour, tables de quelques Mo
- **Lectures fréquentes** : l'API FastAPI interroge les 3 tables d'historique à chaque appel HTTP
- **Calculs relationnels complexes** : les indicateurs sont calculés via des window functions SQL (`LAG`, `PARTITION BY`, `JOIN`) difficiles à exprimer autrement
- **Cohérence transactionnelle requise** : le reset quotidien doit être atomique (DROP + CREATE en une transaction)

## Décision

**PostgreSQL 17** est utilisé comme unique système de stockage, via Docker (`postgres:17-alpine`). SQLAlchemy sert de couche d'abstraction pour les connexions (pooling, portabilité).

## Alternatives considérées

| Alternative | Raison du rejet |
| ----------- | --------------- |
| **DuckDB** | Excellent pour l'analytique en lecture, mais conçu pour un usage mono-process. Le pipeline (écriture) et l'API (lecture) tournent dans des conteneurs distincts — DuckDB en mode fichier partagé introduit des conflits de verrous |
| **SQLite** | Même problème de concurrence multi-processus. Adapté pour les tests unitaires, pas pour la production multi-conteneurs |
| **BigQuery / Snowflake** | Latence réseau et coût disproportionnés pour ~6 lignes/cycle. Ajoute une dépendance cloud externe qui complexifie le déploiement local |


## Conséquences

**Avantages :**
- Window functions SQL natives : les calculs d'intervalles inter-trains (`LAG`) et de scores de régularité sont exprimés directement en SQL, sans couche de transformation Python supplémentaire
- ACID garanti : le reset quotidien (`DROP TABLE / CREATE TABLE AS SELECT`) est atomique
- Compatible avec les outils de l'écosystème data (PgAdmin, SQLAlchemy, psycopg2, dbt si besoin)
- Déploiement trivial via Docker, aucune dépendance cloud

**Inconvénients et risques assumés :**
- Scalabilité horizontale limitée — non pertinent à ce volume
- Les données ne survivent pas si le volume Docker est supprimé — documenté dans le Quick Start
- Si l'historique dépasse plusieurs années de données, une migration vers un stockage analytique (DuckDB en lecture seule, BigQuery) serait à envisager
