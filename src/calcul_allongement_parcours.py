from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_CIBLE = "evolution_ecart_horaire"
TABLE_SOURCE = "detail_ecarts_horaires"

SQL_CREATION = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CIBLE} (
        id               SERIAL PRIMARY KEY,
        mission          TEXT,
        date_observation TIMESTAMP,
        ecart_max        INTEGER
    )
"""

SQL_INSERTION = f"""
    INSERT INTO {TABLE_CIBLE} (mission, date_observation, ecart_max)
    SELECT
        mission,
        NOW() AT TIME ZONE 'Europe/Paris' AS date_observation,
        ecart_max
    FROM {TABLE_SOURCE}
    WHERE ecart_max IS NOT NULL
"""


def creer_table(engine: Engine) -> None:
    """Crée la table evolution_ecart_horaire si elle n'existe pas encore.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION))


def inserer_snapshot(engine: Engine) -> None:
    """Insère un snapshot des écarts horaires courants dans evolution_ecart_horaire.

    Chaque appel ajoute de nouvelles lignes sans écraser les précédentes,
    permettant d'historiser l'évolution de ecart_max par mission.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        resultat = conn.execute(text(SQL_INSERTION))
        print(f"Snapshot inséré : {resultat.rowcount} lignes → {TABLE_CIBLE}")


def main() -> None:
    """Point d'entrée du script.

    Crée la table si nécessaire, puis insère les données courantes
    de detail_ecarts_horaires dans evolution_ecart_horaire.
    """
    engine = creer_engine()
    creer_table(engine)
    inserer_snapshot(engine)


if __name__ == "__main__":
    main()
