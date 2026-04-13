from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_CIBLE = "hist_ecart_horaire"
TABLE_SOURCE = "detail_ecarts_horaires"
TABLE_ALLONGEMENT = "hist_moyenne_allongement"

SQL_CREATION = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CIBLE} (
        id               SERIAL PRIMARY KEY,
        mission          TEXT,
        direction        TEXT,
        date_observation TIMESTAMP,
        ecart_max        INTEGER
    )
"""

SQL_INSERTION = f"""
    INSERT INTO {TABLE_CIBLE} (mission, direction, date_observation, ecart_max)
    SELECT
        mission, direction,
        NOW() AT TIME ZONE 'Europe/Paris' AS date_observation,
        ecart_max
    FROM {TABLE_SOURCE}
    WHERE ecart_max IS NOT NULL
"""

SQL_CREATION_ALLONGEMENT = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ALLONGEMENT} (
        id                   SERIAL PRIMARY KEY,
        date_calcul          TIMESTAMP,
        direction            TEXT,
        moyenne_allongement  FLOAT
    )
"""
# -- On regarde l'augmentation (ou non) du retard par rapport aux horaires théoriques à chaque observation
SQL_INSERTION_ALLONGEMENT = f"""
    INSERT INTO {TABLE_ALLONGEMENT} (date_calcul, direction, moyenne_allongement)
    WITH premiere_derniere AS (
        SELECT
            mission, direction,
            MIN(date_observation) AS premiere_observation,
            MAX(date_observation) AS derniere_observation
        FROM {TABLE_CIBLE}
        GROUP BY 1,2
    ),
    ecarts AS (
        SELECT
            pd.mission,
            pd.direction,
            pd.premiere_observation,
            pd.derniere_observation,
            MAX(pd.derniere_observation) OVER () AS date_max,
            premier.ecart_max AS ecart_premier,
            dernier.ecart_max AS ecart_dernier
        FROM premiere_derniere pd
        JOIN {TABLE_CIBLE} premier
            ON premier.mission           = pd.mission
            AND premier.date_observation = pd.premiere_observation
        JOIN {TABLE_CIBLE} dernier
            ON dernier.mission           = pd.mission
            AND dernier.date_observation = pd.derniere_observation
    ),
    ratio AS (
        SELECT
            mission, direction,
            premiere_observation,
            derniere_observation,
            ecart_premier,
            ecart_dernier,
            (EXTRACT(HOUR  FROM (derniere_observation - premiere_observation)) * 60 +
             EXTRACT(MINUTE FROM (derniere_observation - premiere_observation))) AS temps_circule_min,
            COALESCE(ecart_dernier - ecart_premier, 0) AS evolution_ecart
        FROM ecarts
        WHERE derniere_observation = date_max
    ),
    allongement AS (
        SELECT
            mission, direction, premiere_observation, derniere_observation,
            ecart_premier, ecart_dernier,
            temps_circule_min, evolution_ecart,
            evolution_ecart / (temps_circule_min + 1) AS allongement_parcours
        FROM ratio
    )
    SELECT
        NOW() AT TIME ZONE 'Europe/Paris' AS date_calcul, direction,
        AVG(allongement_parcours)         AS moyenne_allongement
    FROM allongement
    GROUP BY direction
"""


def creer_table(engine: Engine) -> None:
    """Crée la table hist_ecart_horaire si elle n'existe pas encore.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION))


def inserer_snapshot(engine: Engine) -> None:
    """Insère un snapshot des écarts horaires courants dans hist_ecart_horaire.

    Chaque appel ajoute de nouvelles lignes sans écraser les précédentes,
    permettant d'historiser l'évolution de ecart_max par mission.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        resultat = conn.execute(text(SQL_INSERTION))
        print(f"Snapshot inséré : {resultat.rowcount} lignes → {TABLE_CIBLE}")


def creer_table_allongement(engine: Engine) -> None:
    """Crée la table hist_moyenne_allongement si elle n'existe pas encore.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION_ALLONGEMENT))


def inserer_snapshot_allongement(engine: Engine) -> None:
    """Calcule et insère la moyenne d'allongement de parcours dans hist_moyenne_allongement.

    Pour chaque mission encore en circulation, compare le retard à la première
    observation au retard à la dernière, rapporté au temps écoulé.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        resultat = conn.execute(text(SQL_INSERTION_ALLONGEMENT))
        print(f"Snapshot allongement inséré : {resultat.rowcount} ligne → {TABLE_ALLONGEMENT}")


def main() -> None:
    """Point d'entrée du script.

    Crée les tables si nécessaire, puis insère les snapshots courants
    dans hist_ecart_horaire et hist_moyenne_allongement.
    """
    engine = creer_engine()
    creer_table(engine)
    inserer_snapshot(engine)
    creer_table_allongement(engine)
    inserer_snapshot_allongement(engine)


if __name__ == "__main__":
    main()
