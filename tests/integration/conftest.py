"""
Fixtures d'intégration : connexion Postgres réelle.

Ces tests sont marqués `integration` et ignorés par défaut (voir pytest.ini).
Pour les exécuter : pytest -m integration

La fixture `pg_engine` tente une vraie connexion à Postgres via creer_engine().
Si la base n'est pas joignable (CI sans Docker, dev local), le test est sauté.
"""

import pytest
from sqlalchemy import text

from utils.connexion_postgres import creer_engine


@pytest.fixture(scope="session")
def pg_engine():
    """Engine SQLAlchemy connecté à Postgres. Skip si la base est inaccessible."""
    engine = creer_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        pytest.skip(f"Postgres inaccessible : {e}")
    return engine
