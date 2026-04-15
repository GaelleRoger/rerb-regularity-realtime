"""
API REST — RER B Régularité temps réel

Expose les données calculées par le pipeline Airflow via 4 routes :
  GET /health       → vérifie que Postgres est joignable
  GET /regularite   → scores de régularité par gare et direction
  GET /allongement  → allongement moyen de parcours par direction
  GET /ecarts       → écarts horaires moyens/médians/max par direction

L'engine SQLAlchemy est instancié une seule fois au démarrage et réutilisé
pour toutes les requêtes (connexion poolée).
"""

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from utils.connexion_postgres import creer_engine

# Chemin vers la racine du projet (trois niveaux au-dessus de src/api/main.py)
RACINE = Path(__file__).parent.parent.parent

# Chargement des variables d'environnement (.env) — PG_HOST, PG_USER, etc.
load_dotenv(RACINE / ".env")

app = FastAPI(title="RERB Regularity API")

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


@app.get("/regularite")
def regularite():
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
        raise HTTPException(status_code=404, detail="Aucune donnée disponible dans hist_moyenne_regularite")
    return rows


@app.get("/allongement")
def allongement():
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


@app.get("/ecarts")
def ecarts():
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
