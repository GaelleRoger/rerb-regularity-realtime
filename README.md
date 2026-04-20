# RER B — Régularité temps réel

![Python](https://img.shields.io/badge/python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![CI](https://img.shields.io/badge/CI-GitHub_Actions-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)

> **Pipeline ETL temps réel qui calcule et expose la régularité du RER B par branches, à partir des horaires officiels Île-de-France Mobilités.**

---

## 🎬 Démo Live

*(Screenshot ou GIF à ajouter)*

---

## 🎯 Problématique

Lorsque le trafic est annoncé perturbé sur le RER, il est parfois difficile d'estime "de combien" il est perturbé. Les usagers peuvent-ils faire leur trajet habituel moyennant quelques désagréments ou vaut-il mieux qu'ils reportent ou modifient leur voyage ?

**Enjeux :**
Proposer 3 indicateurs mis à jour toutes les 5 minutes pour avoir une meilleure vue d'ensemble du trafic
- La régularité par direction et par branche, on regarde l'intervalle entre les trains
- L'allongement du temps de parcours
- La différence entre horaires réels et théoriques

## 💡 Solution

Un pipeline Python s'exécute en continu via un cron Docker : il interroge l'API IDFM, filtre les données aberrantes, calcule trois indicateurs (régularité, allongement de parcours, écarts horaires) et les stocke en base. Une API FastAPI les expose, un dashboard Streamlit les affiche en temps réel.

---

## ✨ Fonctionnalités

- 🔄 **Pipeline ETL automatique** : extraction, nettoyage et chargement toutes les 5 minutes via cron Docker
- 📊 **Score de régularité par gare** : pourcentage de trains à l'heure aux gares clés (Sceaux, Antony, Châtelet, CDG1…) pour chaque direction
- ⏱️ **Allongement de parcours** : détecte si les trains accumulent du retard en cours de trajet (dérive Nord/Sud)
- 📡 **API REST sécurisée** : 4 routes FastAPI avec authentification par clé, schémas Pydantic et endpoint `/metrics` Prometheus
- 📈 **Monitoring intégré** : Grafana + Prometheus avec dashboard préconstruit (latence p95, req/s, taux d'erreurs, CPU, RAM)

---

## 🛠️ Stack Technique

| Couche          | Technologies                                              |
| --------------- | --------------------------------------------------------- |
| **Ingestion**   | API REST Île-de-France Mobilités, `requests`, `pandas`    |
| **Processing**  | Python 3.12, `pandas`, SQL                                |
| **Stockage**    | PostgreSQL 17                                             |
| **Exposition**  | FastAPI 0.115, Streamlit, Pydantic v2                     |
| **Infra**       | Docker Compose, cron, GitHub Actions CI                   |
| **Monitoring**  | Prometheus, Grafana, `prometheus-fastapi-instrumentator`  |

---

## 🏗️ Architecture

```
Île-de-France Mobilités API
        │
        ▼ (toutes les 5 min)
 ┌──────────────────────┐
 │   Pipeline ETL cron  │
 │  extract_horaires    │
 │  load_postgres       │
 │  creation_table_ref  │
 │  calcul_regularite   │
 │  calcul_ecarts       │
 │  calcul_allongement  │
 └──────────┬───────────┘
            │
            ▼
      ┌──────────┐
      │ PostgreSQL│
      └─────┬────┘
            │
            ▼
      ┌──────────┐        ┌─────────────┐
      │ FastAPI  │◄───────│  Streamlit  │
      │ /metrics │        │  Dashboard  │
      └─────┬────┘        └─────────────┘
            │
            ▼
  ┌──────────────────┐
  │ Prometheus       │
  │ Grafana          │
  └──────────────────┘
```

---

## ⚡ Quick Start

```bash
# 1. Cloner le repository
git clone https://github.com/GaelleRoger/rerb-regularity-realtime.git
cd rerb-regularity-realtime

# 2. Configurer les variables d'environnement
cp .env.example .env
# Renseigner : PG_USER, PG_PASSWORD, PG_DB, API_KEY (IDFM), RERB_API_KEY

# 3. Lancer tous les services
./start.sh
```

⏱️ *Prêt en moins de 2 minutes. Premier calcul de régularité disponible après 5 minutes.*

| Service     | URL                        |
| ----------- | -------------------------- |
| Dashboard   | http://localhost:8501      |
| API         | http://localhost:8000/docs |
| Grafana     | http://localhost:3000      |
| Prometheus  | http://localhost:9090      |
| PgAdmin     | http://localhost:8082      |

### Variables d'environnement requises

| Variable        | Description                                      |
| --------------- | ------------------------------------------------ |
| `PG_USER`       | Utilisateur PostgreSQL                           |
| `PG_PASSWORD`   | Mot de passe PostgreSQL                          |
| `PG_DB`         | Nom de la base de données                        |
| `API_KEY`       | Clé API Île-de-France Mobilités                  |
| `RERB_API_KEY`  | Clé d'authentification de l'API FastAPI interne  |

---

## 📊 Indicateurs calculés

| Indicateur              | Description                                                                 |
| ----------------------- | --------------------------------------------------------------------------- |
| **Score de régularité** | % de trains respectant l'intervalle théorique, par gare clé et par direction |
| **Allongement**         | Dérive moyenne du retard entre le premier et le dernier arrêt d'une mission |
| **Écart horaire**       | Retard moyen, médian et maximum des missions en circulation                  |

---

## 🧪 Tests

```bash
uv run pytest          # Tests unitaires (sans Postgres)
uv run pytest -m integration  # Tests d'intégration (Postgres requis)
```

La CI GitHub Actions exécute les tests unitaires à chaque push sur `main`.

---

## 📅 Roadmap

- [x] Pipeline ETL avec filtrage des données aberrantes (missions RATP, missions terminées)
- [x] API REST sécurisée (clé API, schémas Pydantic, `/metrics`)
- [x] Dashboard Streamlit avec rafraîchissement automatique toutes les 5 min
- [x] Monitoring Prometheus / Grafana avec dashboard préconstruit
- [x] CI GitHub Actions
- [ ] Déploiement sur VM cloud pour démo publique
- [ ] Alertes Grafana (seuil de régularité critique)
- [ ] Historisation longue durée et graphes de tendance sur 7 jours

---

## 🤝 Contact & Collaboration

**Développé par :** Gaëlle Roger
🔗 [LinkedIn](https://linkedin.com/in/gaelle-roger) · 📧 [contact](mailto:gaelle.roger@ikmail.com)

**Vous recrutez en Data Engineering / Architect ?**
Je suis disponible pour en discuter — n'hésitez pas à me contacter directement.

---

⭐ *Ce projet vous a été utile ? Une étoile est toujours appréciée.*
