from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")


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


def supprimer_tables(engine, tables: list[str]) -> None:
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

    Liste les tables existantes, demande confirmation, puis les supprime.
    """
    engine = creer_engine()
    toutes = lister_tables(engine)
    tables = [t for t in toutes if not t.startswith("hist_moyenne_")]
    ignorees = [t for t in toutes if t.startswith("hist_moyenne_")]

    if ignorees:
        print("Tables conservées (hist_moyenne_*) :")
        for t in ignorees:
            print(f"  ~ {t}")

    if not tables:
        print("Aucune table trouvée dans le schéma public.")
        return

    print("Tables qui seront supprimées :")
    for t in tables:
        print(f"  - {t}")

    reponse = input("\nConfirmer la suppression ? [oui/non] : ").strip().lower()
    if reponse != "oui":
        print("Annulé.")
        return

    supprimer_tables(engine, tables)


if __name__ == "__main__":
    main()
