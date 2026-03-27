import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_REF = "referentiel_missions"

SQL_CREATION = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_REF} AS
    WITH init AS (
        SELECT
            type_mission,
            gare_depart,
            nb_arrets_desservis,
            MAX(nb_arrets_desservis) OVER (
                PARTITION BY type_mission
            ) AS nb_arrets_max
        FROM horaires_theoriques
    )
    SELECT DISTINCT
        type_mission,
        gare_depart
    FROM init
    WHERE nb_arrets_desservis = nb_arrets_max
"""


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


def creer_table_referentiel(engine: Engine) -> None:
    """Crée la table referentiel_missions si elle n'existe pas déjà.

    La table est construite à partir de horaires_theoriques : pour chaque
    type de mission, on retient la gare_depart de la mission qui dessert
    le plus grand nombre d'arrêts (mission la plus complète du type).

    Si la table existe déjà, aucune action n'est effectuée.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION))
    print(f"Table '{TABLE_REF}' prête.")


def main() -> None:
    """Point d'entrée du script.

    Crée la table referentiel_missions dans Postgres si elle n'existe pas.
    """
    engine = creer_engine()
    creer_table_referentiel(engine)


if __name__ == "__main__":
    main()
