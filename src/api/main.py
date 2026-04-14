from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent.parent
load_dotenv(RACINE / ".env")

app = FastAPI(title="RERB Regularity API")
engine = creer_engine()


def _fetch(query: str) -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row) for row in result.mappings().all()]


@app.get("/health")
def health():
    """Vérifie que la connexion Postgres est opérationnelle."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/regularite")
def regularite():
    """Retourne les 2 derniers snapshots de régularité."""
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
    """Retourne les 2 derniers snapshots d'allongement de parcours."""
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
    """Retourne les 2 derniers snapshots d'écarts horaires."""
    rows = _fetch("""
        SELECT * FROM hist_moyenne_ecarts
        ORDER BY date_observation DESC
        LIMIT 2
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Aucune donnée disponible dans hist_moyenne_ecarts")
    return rows
