"""
API REST — RER B Régularité temps réel

Expose les données calculées par le pipeline Airflow via 4 routes :
  GET /health       → vérifie que Postgres est joignable (non protégée)
  GET /regularite   → scores de régularité par gare et direction
  GET /allongement  → allongement moyen de parcours par direction
  GET /ecarts       → écarts horaires moyens/médians/max par direction

Authentification : toutes les routes sauf /health requièrent un header
  X-API-Key: <valeur de API_KEY dans .env>
Réponse 401 si le header est absent ou incorrect.

L'engine SQLAlchemy est instancié une seule fois au démarrage et réutilisé
pour toutes les requêtes (connexion poolée).
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, ConfigDict
from sqlalchemy import text

from utils.connexion_postgres import creer_engine

# Chemin vers la racine du projet (trois niveaux au-dessus de src/api/main.py)
RACINE = Path(__file__).parent.parent.parent

# Chargement des variables d'environnement (.env) — PG_HOST, PG_USER, API_KEY, etc.
load_dotenv(RACINE / ".env")

app = FastAPI(title="RERB Regularity API")

# ── Authentification par API key ──────────────────────────────────────────────

# Clé attendue, lue depuis l'environnement. Absence = API non protégée (warning).
_API_KEY = os.environ.get("RERB_API_KEY", "")

# Schéma FastAPI : lit le header HTTP "X-API-Key" sur chaque requête protégée.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verifier_api_key(api_key: str = Security(_api_key_header)) -> None:
    """Dépendance FastAPI : vérifie que le header X-API-Key correspond à API_KEY.

    Lève HTTP 401 si la clé est absente ou incorrecte.
    Lève HTTP 500 si API_KEY n'est pas configurée dans l'environnement.
    """
    if not _API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY non configurée sur le serveur")
    if api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="Clé API invalide ou absente")

# ── Modèles de réponse Pydantic ───────────────────────────────────────────────

class RegulariteRow(BaseModel):
    """Snapshot de régularité pour une direction et une heure d'observation."""

    model_config = ConfigDict(from_attributes=True)

    date_observation: Optional[datetime] = None
    direction: Optional[str] = None
    score_sceaux: Optional[int] = None
    score_antony: Optional[int] = None
    score_bourg_la_reine: Optional[int] = None
    score_chatelet: Optional[int] = None
    score_aulnay: Optional[int] = None
    score_cdg1: Optional[int] = None
    score_vert_galant: Optional[int] = None


class AllongementRow(BaseModel):
    """Snapshot d'allongement moyen de parcours pour une direction."""

    model_config = ConfigDict(from_attributes=True)

    date_calcul: Optional[datetime] = None
    direction: Optional[str] = None
    moyenne_allongement: Optional[float] = None


class EcartsRow(BaseModel):
    """Snapshot des écarts horaires pour une direction."""

    model_config = ConfigDict(from_attributes=True)

    date_observation: Optional[datetime] = None
    direction: Optional[str] = None
    ecart_moyen: Optional[float] = None
    ecart_median: Optional[float] = None
    ecart_max: Optional[float] = None


# Engine SQLAlchemy partagé — évite d'ouvrir une nouvelle connexion à chaque requête
engine = creer_engine()


def _fetch(query: str) -> list[dict]:
    """Exécute une requête SELECT et retourne les résultats sous forme de liste de dicts.

    Args:
        query: Requête SQL à exécuter.

    Returns:
        Liste de dictionnaires, un par ligne retournée.
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row) for row in result.mappings().all()]


@app.get("/health")
def health():
    """Vérifie que la connexion Postgres est opérationnelle.

    Exécute un SELECT 1 minimal. Retourne 200 si OK, 503 si la base
    est inaccessible (conteneur postgres arrêté, mauvaises credentials, etc.).
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/regularite", response_model=list[RegulariteRow])
def regularite(_: None = Depends(verifier_api_key)):
    """Retourne les 2 derniers snapshots de régularité (table hist_moyenne_regularite).

    Chaque ligne contient un score par gare clé (sceaux, antony, chatelet, etc.)
    pour une direction (Nord/Sud) et une heure d'observation.
    Les 2 lignes correspondent généralement aux 2 dernières directions enregistrées.

    Retourne 404 si la table est vide (pipeline pas encore exécuté).
    """
    rows = _fetch("""
        SELECT * FROM hist_moyenne_regularite
        ORDER BY date_observation DESC
        LIMIT 2
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Aucune donnée de régularité disponible")
    return rows


@app.get("/allongement", response_model=list[AllongementRow])
def allongement(_: None = Depends(verifier_api_key)):
    """Retourne les 2 derniers snapshots d'allongement de parcours (table hist_moyenne_allongement).

    L'allongement mesure si les trains accumulent du retard au fil de leur trajet :
    une valeur positive indique que l'écart au théorique s'aggrave avec le temps.
    Résultat exprimé en minutes de retard supplémentaire par minute de circulation.

    Retourne 404 si la table est vide.
    """
    rows = _fetch("""
        SELECT * FROM hist_moyenne_allongement
        ORDER BY date_calcul DESC
        LIMIT 2
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible dans hist_moyenne_allongement")
    return rows


@app.get("/ecarts", response_model=list[EcartsRow])
def ecarts(_: None = Depends(verifier_api_key)):
    """Retourne les 2 derniers snapshots d'écarts horaires (table hist_moyenne_ecarts).

    Chaque ligne contient pour une direction :
      - ecart_moyen  : moyenne des écarts max par mission (en minutes)
      - ecart_median : médiane des écarts max par mission (en minutes)
      - ecart_max    : pire écart observé toutes missions confondues (en minutes)

    Retourne 404 si la table est vide.
    """
    rows = _fetch("""
        SELECT * FROM hist_moyenne_ecarts
        ORDER BY date_observation DESC
        LIMIT 2
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible dans hist_moyenne_ecarts")
    return rows
