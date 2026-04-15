"""
Tests unitaires pour src/load_postgres.py.

Couvre :
- normaliser_nom_colonne() : nettoyage des noms de colonnes
- trouver_dernier_csv()    : sélection du CSV le plus récent dans un dossier
- est_deja_charge()        : consultation de la table de log (SQLite en mémoire)
- enregistrer_fichier()    : insertion dans la table de log (SQLite en mémoire)

Les tests de base de données utilisent un engine SQLite en mémoire pour
ne pas dépendre d'une instance Postgres active.
"""

import time
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from load_postgres import (
    enregistrer_fichier,
    est_deja_charge,
    normaliser_nom_colonne,
    trouver_dernier_csv,
)


# ── Fixture SQLite ────────────────────────────────────────────────────────────

@pytest.fixture
def engine_sqlite():
    """Engine SQLite en mémoire avec la table fichiers_charges créée."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE fichiers_charges (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nom_fichier TEXT NOT NULL UNIQUE,
                charge_le   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    return engine


# ── normaliser_nom_colonne ────────────────────────────────────────────────────

def test_normaliser_accent():
    assert normaliser_nom_colonne("Rémy") == "remy"


def test_normaliser_tiret():
    assert normaliser_nom_colonne("saint-remy") == "saint_remy"


def test_normaliser_espace():
    assert normaliser_nom_colonne("gare depart") == "gare_depart"


def test_normaliser_parentheses():
    assert normaliser_nom_colonne("heure (depart)") == "heure"


def test_normaliser_minuscules():
    assert normaliser_nom_colonne("GARE") == "gare"


def test_normaliser_underscores_multiples():
    assert normaliser_nom_colonne("gare__depart") == "gare_depart"


def test_normaliser_underscore_bord():
    assert normaliser_nom_colonne("_gare_") == "gare"


def test_normaliser_nom_complexe():
    assert normaliser_nom_colonne("Saint-Rémy-lès-Chevreuse") == "saint_remy_les_chevreuse"


# ── trouver_dernier_csv ───────────────────────────────────────────────────────

def test_trouver_dernier_csv_retourne_le_plus_recent(tmp_path):
    """Le fichier avec la date de modification la plus récente doit être retourné."""
    ancien = tmp_path / "horaires_theoriques_20260101_080000.csv"
    recent = tmp_path / "horaires_theoriques_20260415_120000.csv"
    ancien.write_text("data")
    time.sleep(0.01)  # assure un mtime distinct
    recent.write_text("data")

    with patch("load_postgres.DOSSIER_RAW", tmp_path):
        resultat = trouver_dernier_csv("horaires_theoriques")

    assert resultat == recent


def test_trouver_dernier_csv_aucun_fichier(tmp_path):
    """Retourne None quand aucun fichier ne correspond au préfixe."""
    with patch("load_postgres.DOSSIER_RAW", tmp_path):
        resultat = trouver_dernier_csv("horaires_theoriques")

    assert resultat is None


def test_trouver_dernier_csv_ignore_mauvais_prefixe(tmp_path):
    """Un fichier avec un autre préfixe ne doit pas être retourné."""
    (tmp_path / "horaires_reels_20260415_120000.csv").write_text("data")

    with patch("load_postgres.DOSSIER_RAW", tmp_path):
        resultat = trouver_dernier_csv("horaires_theoriques")

    assert resultat is None


def test_trouver_dernier_csv_ignore_format_invalide(tmp_path):
    """Un fichier dont le nom ne suit pas le pattern est ignoré."""
    (tmp_path / "horaires_theoriques_mauvais.csv").write_text("data")

    with patch("load_postgres.DOSSIER_RAW", tmp_path):
        resultat = trouver_dernier_csv("horaires_theoriques")

    assert resultat is None


# ── est_deja_charge / enregistrer_fichier ────────────────────────────────────

def test_est_deja_charge_false_si_absent(engine_sqlite):
    """Un fichier non encore enregistré doit retourner False."""
    assert est_deja_charge(engine_sqlite, "horaires_theoriques_20260415_120000.csv") is False


def test_enregistrer_puis_verifier(engine_sqlite):
    """Après enregistrement, est_deja_charge doit retourner True."""
    nom = "horaires_theoriques_20260415_120000.csv"
    enregistrer_fichier(engine_sqlite, nom)
    assert est_deja_charge(engine_sqlite, nom) is True


def test_enregistrer_doublon_leve_exception(engine_sqlite):
    """Enregistrer deux fois le même fichier doit lever une exception (contrainte UNIQUE)."""
    nom = "horaires_theoriques_20260415_120000.csv"
    enregistrer_fichier(engine_sqlite, nom)
    with pytest.raises(Exception):
        enregistrer_fichier(engine_sqlite, nom)


def test_est_deja_charge_distincts(engine_sqlite):
    """Deux fichiers différents sont traités indépendamment."""
    nom_a = "horaires_theoriques_20260101_080000.csv"
    nom_b = "horaires_theoriques_20260415_120000.csv"
    enregistrer_fichier(engine_sqlite, nom_a)
    assert est_deja_charge(engine_sqlite, nom_a) is True
    assert est_deja_charge(engine_sqlite, nom_b) is False
