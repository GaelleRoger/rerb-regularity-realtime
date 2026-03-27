import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

DOSSIER_RAW = RACINE / "data/raw"
TABLE_LOG = "fichiers_charges"

PREFIXES = ["horaires_theoriques", "horaires_reels"]


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


def creer_table_log(engine: Engine) -> None:
    """Crée la table de log des fichiers chargés si elle n'existe pas.

    La table fichiers_charges retient le nom de chaque fichier CSV
    déjà inséré en base, pour éviter les doublons.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_LOG} (
                id          SERIAL PRIMARY KEY,
                nom_fichier TEXT NOT NULL UNIQUE,
                charge_le   TIMESTAMP DEFAULT NOW()
            )
        """))


def est_deja_charge(engine: Engine, nom_fichier: str) -> bool:
    """Vérifie si un fichier CSV a déjà été chargé en base.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        nom_fichier: Nom du fichier CSV (sans chemin).

    Returns:
        True si le fichier est déjà présent dans fichiers_charges.
    """
    with engine.connect() as conn:
        resultat = conn.execute(
            text(f"SELECT 1 FROM {TABLE_LOG} WHERE nom_fichier = :nom"),
            {"nom": nom_fichier},
        )
        return resultat.fetchone() is not None


def enregistrer_fichier(engine: Engine, nom_fichier: str) -> None:
    """Enregistre un fichier CSV dans la table de log après chargement.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        nom_fichier: Nom du fichier CSV (sans chemin).
    """
    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO {TABLE_LOG} (nom_fichier) VALUES (:nom)"),
            {"nom": nom_fichier},
        )


def trouver_dernier_csv(prefixe: str) -> Optional[Path]:
    """Retourne le chemin du CSV le plus récent pour un préfixe donné.

    La sélection se base sur le timestamp YYYYMMDD_HHMMSS présent dans
    le nom du fichier (format : {prefixe}_YYYYMMDD_HHMMSS.csv).

    Args:
        prefixe: Préfixe du fichier à rechercher (ex: 'horaires_theoriques').

    Returns:
        Chemin vers le fichier le plus récent, ou None si aucun fichier trouvé.
    """
    pattern = re.compile(rf"^{re.escape(prefixe)}_(\d{{8}}_\d{{6}})\.csv$")
    candidats = [
        (m.group(1), f)
        for f in DOSSIER_RAW.iterdir()
        if (m := pattern.match(f.name))
    ]
    if not candidats:
        return None
    _, chemin = max(candidats, key=lambda x: x[0])
    return chemin


def charger_csv_en_base(engine: Engine, chemin: Path, table: str) -> None:
    """Charge un fichier CSV dans une table Postgres.

    Insère les données en mode append (la table existante est conservée).
    Enregistre ensuite le fichier dans fichiers_charges pour éviter
    un double chargement futur.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        chemin: Chemin complet vers le fichier CSV.
        table: Nom de la table cible dans Postgres.
    """
    nom_fichier = chemin.name

    if est_deja_charge(engine, nom_fichier):
        print(f"Déjà chargé, ignoré : {nom_fichier}")
        return

    df = pd.read_csv(chemin)
    df.to_sql(table, engine, if_exists="append", index=False)
    enregistrer_fichier(engine, nom_fichier)
    print(f"Chargé ({len(df)} lignes) : {nom_fichier} → {table}")


def main() -> None:
    """Point d'entrée du script.

    Pour chaque préfixe (horaires_theoriques, horaires_reels) :
    - identifie le CSV le plus récent dans data/raw/
    - le charge dans la table Postgres correspondante si non déjà chargé
    """
    engine = creer_engine()
    creer_table_log(engine)

    for prefixe in PREFIXES:
        chemin = trouver_dernier_csv(prefixe)
        if chemin is None:
            print(f"Aucun fichier trouvé pour le préfixe : {prefixe}")
            continue
        charger_csv_en_base(engine, chemin, table=prefixe)


if __name__ == "__main__":
    main()
