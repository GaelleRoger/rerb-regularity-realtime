from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_REF = "referentiel_missions"
TABLE_TMP = "referentiel_missions_tmp"
TABLE_BUFFER = "referentiel_missions_buffer"

SQL_CREATION_REF  = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_REF} AS
    WITH init AS (
        SELECT
            type_mission as code_mission,
            gare_depart,
            destination as gare_destination,
            nb_arrets_desservis,
            MAX(nb_arrets_desservis) OVER (
                PARTITION BY type_mission
            ) AS nb_arrets_max
        FROM horaires_theoriques_trv
    )
    SELECT DISTINCT
        code_mission,
        gare_depart, gare_destination,
        nb_arrets_max as nb_arrets_mission
    FROM init
    WHERE nb_arrets_desservis = nb_arrets_max
"""

SQL_CREATION_TMP = f"""
    CREATE TABLE {TABLE_TMP} AS
    WITH init AS (
        SELECT
            type_mission as code_mission,
            gare_depart, destination as gare_destination,
            nb_arrets_desservis,
            date_observation,
            MAX(nb_arrets_desservis) OVER (
                PARTITION BY type_mission
            ) AS nb_arrets_max,
            MAX(date_observation) OVER () AS date_max
        FROM horaires_theoriques_trv
    )
    SELECT DISTINCT
        code_mission,
        gare_depart, gare_destination,
        nb_arrets_max as nb_arrets_mission
    FROM init
    WHERE nb_arrets_desservis = nb_arrets_max
    AND date_observation = date_max
"""

SQL_CREATION_BUFFER  = f"""
    CREATE TABLE {TABLE_BUFFER} AS
    WITH init as (select * from referentiel_missions),
    tmp as (SELECT code_mission as mission_tmp, gare_depart as depart_tmp,
    gare_destination as destination_tmp, nb_arrets_mission as nb_tmp
    FROM referentiel_missions_tmp)
    SELECT COALESCE(code_mission, mission_tmp) as code_mission,
    COALESCE(gare_depart,depart_tmp) as gare_depart,
    COALESCE(gare_destination, destination_tmp) as gare_destination,
    COALESCE(nb_arrets_mission, nb_tmp) as nb_arrets_mission
    FROM init
    FULL OUTER JOIN tmp
    ON init.code_mission = tmp.mission_tmp
"""

SQL_MAJ_REF = f"""
    CREATE TABLE {TABLE_REF} AS
    SELECT *
    FROM {TABLE_BUFFER}
"""


def table_existe(engine: Engine, nom_table: str) -> bool:
    """Vérifie si une table existe dans le schéma public.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        nom_table: Nom de la table à vérifier.

    Returns:
        True si la table existe, False sinon.
    """
    with engine.connect() as conn:
        resultat = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = :nom
        """), {"nom": nom_table})
        return resultat.fetchone() is not None


def creer_table_referentiel(engine: Engine) -> None:
    """Crée la table referentiel_missions si elle n'existe pas.

    La table est construite à partir de horaires_theoriques : pour chaque
    type de mission, on retient la gare_depart de la mission qui dessert
    le plus grand nombre d'arrêts (mission la plus complète du type).

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION_REF))
    print(f"Table '{TABLE_REF}' créée.")


def creer_table_tmp(engine: Engine) -> None:
    """Crée (ou recrée) la table referentiel_missions_tmp.

    La table est construite à partir du snapshot le plus récent de
    horaires_theoriques (filtré sur le MAX de date_observation), en
    retenant pour chaque type de mission la gare_depart de la mission
    qui dessert le plus grand nombre d'arrêts.

    Si referentiel_missions_tmp existe déjà, elle est supprimée avant
    d'être recréée.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_TMP}"))
        conn.execute(text(SQL_CREATION_TMP))
    print(f"Table '{TABLE_TMP}' recréée.")


def creer_table_buffer(engine: Engine) -> None:
    """Crée (ou recrée) la table referentiel_missions_buffer.

    Cette table contient la mise à jour de la table référentiel avant sa mise à jour

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_BUFFER}"))
        conn.execute(text(SQL_CREATION_BUFFER))
    print(f"Table '{TABLE_BUFFER}' recréée.")


def maj_table_referentiel(engine: Engine) -> None:
    """Met à jour la table referentiel_missions.

    La table est mise à jour en incluant les nouveaux codes missions contenus dans referentiel_missions_tmp.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_REF}"))
        conn.execute(text(SQL_MAJ_REF))
    print(f"Table '{TABLE_REF}' mise à jour.")


def main() -> None:
    """Point d'entrée du script.

    - Si referentiel_missions n'existe pas : la crée. (Créé une fois par jour)
    - Si referentiel_missions existe déjà : crée (ou recrée)
      referentiel_missions_tmp à partir du snapshot le plus récent.
      referentiel_missions_buffer : mise à jour référentiel avec table tmp
      puis mise à jour du référentiel final
    """
    engine = creer_engine()

    if not table_existe(engine, TABLE_REF):
        creer_table_referentiel(engine)
    else:
        print(f"Table '{TABLE_REF}' déjà existante.")
        creer_table_tmp(engine)
        creer_table_buffer(engine)
        maj_table_referentiel(engine)


if __name__ == "__main__":
    main()
