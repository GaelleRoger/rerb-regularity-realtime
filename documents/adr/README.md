# Architecture Decision Records

Ce dossier documente les décisions architecturales significatives prises dans le projet **RER B — Régularité temps réel**.

Chaque ADR suit le format : `YYYY-MM-DD-titre-court.md`

## Index

| ADR | Date | Décision | Statut |
| --- | ---- | -------- | ------ |
| [ADR-001](2026-04-26-airflow-vs-cron-docker.md) | 2026-04-26 | Orchestration : Airflow → cron Docker | ✅ Accepté |
| [ADR-002](2026-04-26-postgresql-stockage-central.md) | 2026-04-26 | Stockage : PostgreSQL comme base centrale | ✅ Accepté |
| [ADR-003](2026-04-26-api-key-vs-oauth2.md) | 2026-04-26 | Authentification API : clé statique vs OAuth2/JWT | ✅ Accepté |
| [ADR-004](2026-04-26-prometheus-grafana-self-hosted.md) | 2026-04-26 | Monitoring : self-hosted vs solution managée | ✅ Accepté |

## Format d'un ADR

```
# ADR-XXX : [Titre de la décision]

## Statut
[Proposé | Accepté | Déprécié | Remplacé par ADR-XXX]

## Contexte
[Pourquoi cette décision était nécessaire. Contraintes, faits, forces en présence.]

## Décision
[Ce qui a été décidé, formulé de manière affirmative.]

## Alternatives considérées
[Options évaluées et raisons de leur rejet.]

## Conséquences
[Ce que cette décision implique : avantages, inconvénients, risques assumés.]
```
