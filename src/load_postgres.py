import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

DOSSIER_RAW = RACINE / "data/raw"
TABLE_LOG = "fichiers_charges"

PREFIXES = ["horaires_theoriques", "horaires_reels"]

COLONNES_META = ["type_mission", "destination", "gare_depart", "nb_arrets_desservis"]


def normaliser_nom_colonne(nom: str) -> str:
    """Normalise un nom de colonne pour le rendre compatible Postgres.

    Transformations appliquées dans l'ordre :
    - Suppression des parenthèses et de leur contenu.
    - Suppression des accents (Unicode NFKD + filtre sur les caractères de base).
    - Mise en minuscules.
    - Remplacement des espaces et tirets par des underscores.
    - Suppression des caractères non alphanumériques résiduels.
    - Compression des underscores multiples en un seul.
    - Suppression des underscores en début et fin.

    Args:
        nom: Nom de colonne original (ex: 'Saint-Rémy-lès-Chevreuse').

    Returns:
        Nom normalisé (ex: 'saint_remy_les_chevreuse').
    """
    nom = re.sub(r"\(.*?\)", "", nom)
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c))
    nom = nom.lower()
    nom = re.sub(r"[ \-]", "_", nom)
    nom = re.sub(r"[^\w]", "", nom)
    nom = re.sub(r"_+", "_", nom)
    return nom.strip("_")


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

    Un seul enregistrement couvre les insertions dans les tables _trv
    et slim issues du même fichier source.

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

    La sélection se base sur la date de modification du fichier, ce qui
    reste correct quel que soit le fuseau horaire utilisé dans le nom.

    Args:
        prefixe: Préfixe du fichier à rechercher (ex: 'horaires_theoriques').

    Returns:
        Chemin vers le fichier le plus récent, ou None si aucun fichier trouvé.
    """
    pattern = re.compile(rf"^{re.escape(prefixe)}_\d{{8}}_\d{{6}}\.csv$")
    candidats = [f for f in DOSSIER_RAW.iterdir() if pattern.match(f.name)]
    if not candidats:
        return None
    return max(candidats, key=lambda f: f.stat().st_mtime)


def charger_csv_en_base(
    engine: Engine,
    chemin: Path,
    table: str,
    colonnes_a_exclure: list[str],
) -> None:
    """Charge un fichier CSV dans une table Postgres après filtrage des colonnes.

    Insère les données en mode append (la table existante est conservée).
    Les colonnes listées dans colonnes_a_exclure sont retirées avant insertion.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
        chemin: Chemin complet vers le fichier CSV.
        table: Nom de la table cible dans Postgres.
        colonnes_a_exclure: Liste des colonnes à ne pas insérer.
            Passer une liste vide pour insérer toutes les colonnes.
    """
    lignes_brutes = sum(1 for _ in chemin.open()) - 1  # -1 pour l'en-tête
    df = pd.read_csv(chemin, on_bad_lines="skip")
    lignes_skippees = lignes_brutes - len(df)
    if lignes_skippees > 0:
        print(f"[ATTENTION] {lignes_skippees} ligne(s) ignorée(s) (format invalide) : {chemin.name}")
    colonnes_presentes = [c for c in colonnes_a_exclure if c in df.columns]
    df = df.drop(columns=colonnes_presentes)
    df = df.rename(columns=normaliser_nom_colonne)
    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"Chargé ({len(df)} lignes) : {chemin.name} → {table}")


def main() -> None:
    """Point d'entrée du script.

    Pour chaque préfixe (horaires_theoriques, horaires_reels) :
    - Identifie le CSV le plus récent dans data/raw/.
    - Si non déjà chargé :
        - Insère toutes les colonnes dans la table _trv (travail).
        - Insère les colonnes sans méta-données dans la table slim.
    - Enregistre le fichier dans fichiers_charges (anti-doublon commun).
    """
    engine = creer_engine()
    creer_table_log(engine)

    for prefixe in PREFIXES:
        chemin = trouver_dernier_csv(prefixe)
        if chemin is None:
            print(f"Aucun fichier trouvé pour le préfixe : {prefixe}")
            continue

        if est_deja_charge(engine, chemin.name):
            print(f"Déjà chargé, ignoré : {chemin.name}")
            continue

        charger_csv_en_base(engine, chemin, table=f"{prefixe}_trv", colonnes_a_exclure=[])
        charger_csv_en_base(engine, chemin, table=prefixe, colonnes_a_exclure=COLONNES_META)
        enregistrer_fichier(engine, chemin.name)


if __name__ == "__main__":
    main()
