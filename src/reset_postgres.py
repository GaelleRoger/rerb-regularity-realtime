from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

DOSSIER_EXPORT = RACINE / "data/processed"


def lister_tables(engine) -> list[str]:
    """Retourne la liste des tables présentes dans le schéma public.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.

    Returns:
        Liste des noms de tables.
    """
    with engine.connect() as conn:
        resultat = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """))
        return [row[0] for row in resultat]


def exporter_table_csv(engine: Engine, table: str, dossier: Path) -> None:
    """Exporte le contenu d'une table Postgres dans un fichier CSV horodaté.

    Le fichier est créé même si la table est vide (en-tête seul).
    Le nom suit le format {table}_{YYYYMMDD_HHMM}.csv.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        table: Nom de la table à exporter.
        dossier: Dossier de destination du fichier CSV.
    """
    horodatage = datetime.now().strftime("%Y%m%d_%H%M")
    chemin = dossier / f"{table}_{horodatage}.csv"
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    df.to_csv(chemin, index=False)
    print(f"Exporté ({len(df)} lignes) : {table} → {chemin.name}")


def exporter_toutes_tables(engine: Engine, tables: list[str], dossier: Path) -> None:
    """Exporte toutes les tables listées en CSV dans le dossier cible.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        tables: Liste des noms de tables à exporter.
        dossier: Dossier de destination des fichiers CSV.
    """
    dossier.mkdir(parents=True, exist_ok=True)
    for table in tables:
        exporter_table_csv(engine, table, dossier)


def supprimer_tables(engine: Engine, tables: list[str]) -> None:
    """Supprime toutes les tables listées en une seule requête.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        tables: Liste des noms de tables à supprimer.
    """
    noms = ", ".join(tables)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {noms} CASCADE"))
    print(f"{len(tables)} table(s) supprimée(s) : {noms}")


def main() -> None:
    """Point d'entrée du script.

    Exporte toutes les tables en CSV, puis supprime celles qui ne sont
    pas des tables hist_moyenne_*.
    """
    engine = creer_engine()
    toutes = lister_tables(engine)
    tables = [t for t in toutes if not t.startswith("hist_moyenne_")]

    if not toutes:
        print("Aucune table trouvée dans le schéma public.")
        return

    print(f"Export CSV de {len(toutes)} table(s) vers {DOSSIER_EXPORT} :")
    exporter_toutes_tables(engine, toutes, DOSSIER_EXPORT)

    if not tables:
        print("Aucune table à supprimer.")
        return

    print("Tables qui seront supprimées :")
    for t in tables:
        print(f"  - {t}")

    supprimer_tables(engine, tables)


if __name__ == "__main__":
    main()
