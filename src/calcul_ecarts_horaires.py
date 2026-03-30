from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_ECARTS = "detail_ecarts_horaires"
TABLE_MOYENNE_ECARTS = "hist_moyenne_ecarts"
TABLE_TH = "horaires_theoriques_trv"
TABLE_REEL = "horaires_reels_trv"
TABLE_REF = "referentiel_missions"

COLONNES_META = {
    "mission", "type_mission", "destination",
    "gare_depart", "nb_arrets_desservis", "date_observation",
}


def recuperer_colonnes_gares(engine: Engine) -> list[str]:
    """Récupère la liste ordonnée des colonnes gares depuis la table théorique.

    Toutes les colonnes sont retournées sauf les colonnes méta
    (mission, type_mission, destination, gare_depart,
    nb_arrets_desservis, date_observation).

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.

    Returns:
        Liste des noms de colonnes gares dans l'ordre de la table.
    """
    with engine.connect() as conn:
        resultat = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table
            ORDER BY ordinal_position
        """), {"table": TABLE_TH})
        return [
            row[0] for row in resultat
            if row[0] not in COLONNES_META
        ]


def generer_sql_ecarts(colonnes_gares: list[str]) -> str:
    """Génère dynamiquement la requête SQL de calcul des écarts.

    Pour chaque gare, calcule en minutes l'écart entre l'horaire réel
    et l'horaire théorique sur le dernier snapshot disponible.
    Seules les missions en cours (nb_arrets_desservis < nb_arrets_mission)
    sont incluses.

    La table résultat contient une colonne ecart_max qui vaut le MAX
    des écarts observés sur toutes les gares desservies pour chaque mission
    (les NULLs sont ignorés).

    Args:
        colonnes_gares: Liste des noms de colonnes gares normalisés.

    Returns:
        Requête SQL complète prête à être exécutée.
    """
    cols_select_th = ",\n           ".join(
        f'"{c}"' for c in colonnes_gares
    )
    cols_select_compact = ",\n           ".join(
        f'c."{c}"' for c in colonnes_gares
    )
    cols_ecarts = ",\n    ".join(
        f'ROUND(EXTRACT(EPOCH FROM (\n'
        f'        MAX("{c}"::timestamptz) FILTER (WHERE type_horaire = \'reel\')\n'
        f'      - MAX("{c}"::timestamptz) FILTER (WHERE type_horaire = \'theorique\')\n'
        f'    )) / 60)::INTEGER AS "{c}"'
        for c in colonnes_gares
    )

    array_cols = ", ".join(f'"{c}"' for c in colonnes_gares)

    return f"""
    CREATE TABLE {TABLE_ECARTS} AS
    WITH
    th AS (
        SELECT
            mission, 'theorique' AS type_horaire, nb_arrets_desservis,
            {cols_select_th}
        FROM {TABLE_TH}
        WHERE date_observation = (SELECT MAX(date_observation) FROM {TABLE_TH})
    ),
    reel AS (
        SELECT
            mission, 'reel' AS type_horaire, nb_arrets_desservis,
            {cols_select_th}
        FROM {TABLE_REEL}
        WHERE date_observation = (SELECT MAX(date_observation) FROM {TABLE_REEL})
    ),
    concat AS (
        SELECT * FROM th
        UNION ALL
        SELECT * FROM reel
    ),
    compact AS (
        SELECT
            c.mission, c.type_horaire, c.nb_arrets_desservis,
            {cols_select_compact}
        FROM concat c
        INNER JOIN {TABLE_REF} r
            ON SUBSTRING(c.mission, 1, 4) = r.code_mission
        WHERE c.nb_arrets_desservis < r.nb_arrets_mission
    ),
    ecarts AS (
        SELECT
            mission,
            {cols_ecarts}
        FROM compact
        GROUP BY mission
    )
    SELECT
        *,
        (SELECT MAX(x) FROM UNNEST(ARRAY[{array_cols}]) AS x) AS ecart_max
    FROM ecarts
    ORDER BY mission
    """


def creer_table_ecarts(engine: Engine) -> None:
    """Crée (ou recrée) la table ecarts_horaires.

    Supprime la table existante puis la recrée à partir du dernier
    snapshot disponible dans horaires_theoriques_trv et horaires_reels_trv.
    L'écart est exprimé en minutes entières (positif = retard).

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    colonnes_gares = recuperer_colonnes_gares(engine)
    sql = generer_sql_ecarts(colonnes_gares)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_ECARTS}"))
        conn.execute(text(sql))

    print(f"Table '{TABLE_ECARTS}' recréée ({len(colonnes_gares)} gares).")


def creer_table_histo_ecarts(engine: Engine) -> None:
    """Crée la table historique des écarts horaires.

    A chaque extraction horaire, on calcule la moyenne des écarts horaires 
    sur toutes les missions en circulation. 

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_MOYENNE_ECARTS} AS
            WITH init AS (
            SELECT mission, 
            CASE WHEN ecart_max < 0 THEN 0 ELSE ecart_max END AS ecart_max
            FROM detail_ecarts_horaires
            WHERE ecart_max is not null
            )
            SELECT NOW() AT TIME ZONE 'Europe/Paris' as date_observation, AVG(ecart_max)
            FROM init
            GROUP BY 1
        """))

    print(f"Table '{TABLE_MOYENNE_ECARTS}' créée.")

def charger_table_histo_ecarts(engine: Engine) -> None:
    """Met à jour la table historique des écarts horaires.

    A chaque extraction horaire, on calcule la moyenne des écarts horaires 
    sur toutes les missions en circulation. Cet écart moyen est historisé afin de suivre l'évolution.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {TABLE_MOYENNE_ECARTS}
            WITH init AS (
            SELECT mission, 
            CASE WHEN ecart_max < 0 THEN 0 ELSE ecart_max END AS ecart_max
            FROM detail_ecarts_horaires
            WHERE ecart_max is not null
            )
            SELECT NOW() AT TIME ZONE 'Europe/Paris' as date_observation, AVG(ecart_max)
            FROM init
            GROUP BY 1
        """))

    print(f"Table '{TABLE_MOYENNE_ECARTS}' mise à jour.")


def main() -> None:
    """Point d'entrée du script.

    Calcule les écarts entre horaires réels et théoriques pour chaque
    mission et chaque gare, et les stocke dans ecarts_horaires.
    """
    engine = creer_engine()
    creer_table_ecarts(engine)
    creer_table_histo_ecarts(engine)
    charger_table_histo_ecarts(engine)


if __name__ == "__main__":
    main()
