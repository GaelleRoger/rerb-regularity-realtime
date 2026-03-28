import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def creer_engine() -> Engine:
    """Crée et retourne un engine SQLAlchemy connecté à Postgres.

    La configuration est lue depuis les variables d'environnement :
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB.

    Returns:
        Engine SQLAlchemy prêt à l'emploi.
    """
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    db = os.getenv("PG_DB", "rerb_realtime")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)