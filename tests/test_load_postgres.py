"""
Tests unitaires pour src/load_postgres.py.

Couvre :
- normaliser_nom_colonne() : nettoyage des noms de colonnes
- trouver_dernier_csv()    : sélection du CSV le plus récent dans un dossier
- est_deja_charge()        : consultation de la table de log (SQLite en mémoire)
- enregistrer_fichier()    : insertion dans la table de log (SQLite en mémoire)
- charger_csv_en_base()    : filtre des lignes RATP avant insertion

Les tests de base de données utilisent un engine SQLite en mémoire pour
ne pas dépendre d'une instance Postgres active.
"""

import time
from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from load_postgres import (
    charger_csv_en_base,
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


# ── charger_csv_en_base — filtre RATP ────────────────────────────────────────

@pytest.fixture
def engine_table(engine_sqlite):
    """Engine SQLite avec une table cible pour les tests d'insertion."""
    with engine_sqlite.begin() as conn:
        conn.execute(text("CREATE TABLE horaires_test (mission TEXT)"))
    return engine_sqlite


def test_filtre_ratp_supprime_doublons(tmp_path, engine_table):
    """Les lignes dont la mission commence par RATP doivent être supprimées."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text("mission\nLORE90\nRATOPAL72\nKALI72\nRATPLORE90\n", encoding="utf-8")

    charger_csv_en_base(engine_table, csv, "horaires_test", colonnes_a_exclure=[])

    with engine_table.connect() as conn:
        rows = conn.execute(text("SELECT mission FROM horaires_test")).fetchall()
    missions = [r[0] for r in rows]
    assert "RATPOPAL72" not in missions
    assert "RATPLORE90" not in missions
    assert "LORE90" in missions
    assert "KALI72" in missions


def test_filtre_ratp_conserve_lignes_correctes(tmp_path, engine_table):
    """Une mission sans préfixe RATP doit être insérée normalement."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text("mission\nBIPA84\nELYS66\n", encoding="utf-8")

    charger_csv_en_base(engine_table, csv, "horaires_test", colonnes_a_exclure=[])

    with engine_table.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM horaires_test")).scalar()
    assert count == 2


def test_filtre_ratp_sans_colonne_mission(tmp_path, engine_sqlite):
    """Le filtre RATP ne doit pas lever d'exception si la colonne mission est absente."""
    with engine_sqlite.begin() as conn:
        conn.execute(text("CREATE TABLE slim_test (gare TEXT)"))
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text("gare\nChatelet\nAntony\n", encoding="utf-8")

    # Ne doit pas lever d'exception
    charger_csv_en_base(engine_sqlite, csv, "slim_test", colonnes_a_exclure=[])


# ── charger_csv_en_base — filtre missions terminées ──────────────────────────

@pytest.fixture
def engine_missions(engine_sqlite):
    """Engine SQLite avec une table mission+gare pour les tests de terminus."""
    with engine_sqlite.begin() as conn:
        conn.execute(text("CREATE TABLE horaires_terminus (mission TEXT, chatelet TEXT)"))
    return engine_sqlite


def test_filtre_terminus_supprime_mission_terminee(tmp_path, engine_missions):
    """Une mission dont tous les passages sont dans le passé doit être supprimée."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text(
        "mission,chatelet\n"
        "LORE90,2000-01-01T08:00:00+00:00\n"   # passé lointain → supprimée
        "KALI72,2099-12-31T23:00:00+00:00\n",  # futur → conservée
        encoding="utf-8",
    )

    charger_csv_en_base(engine_missions, csv, "horaires_terminus", colonnes_a_exclure=[])

    with engine_missions.connect() as conn:
        rows = conn.execute(text("SELECT mission FROM horaires_terminus")).fetchall()
    missions = [r[0] for r in rows]
    assert "LORE90" not in missions
    assert "KALI72" in missions


def test_filtre_terminus_conserve_mission_en_cours(tmp_path, engine_missions):
    """Une mission dont le terminus est dans le futur doit être conservée."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text(
        "mission,chatelet\n"
        "BIPA84,2099-12-31T23:00:00+00:00\n",
        encoding="utf-8",
    )

    charger_csv_en_base(engine_missions, csv, "horaires_terminus", colonnes_a_exclure=[])

    with engine_missions.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM horaires_terminus")).scalar()
    assert count == 1


def test_filtre_terminus_sans_heure_valide_conserve_ligne(tmp_path, engine_missions):
    """Une ligne sans aucune heure parseable ne doit pas être supprimée."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text(
        "mission,chatelet\n"
        "ELYS66,\n",  # colonne horaire vide → NaT → conservée
        encoding="utf-8",
    )

    charger_csv_en_base(engine_missions, csv, "horaires_terminus", colonnes_a_exclure=[])

    with engine_missions.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM horaires_terminus")).scalar()
    assert count == 1


def test_filtres_ratp_et_terminus_combines(tmp_path, engine_missions):
    """Les deux filtres (RATP + terminus) s'appliquent indépendamment."""
    csv = tmp_path / "horaires_theoriques_20260415_120000.csv"
    csv.write_text(
        "mission,chatelet\n"
        "RATPLORE90,2099-12-31T23:00:00+00:00\n"  # RATP → supprimée
        "LORE90,2000-01-01T08:00:00+00:00\n"       # terminée → supprimée
        "KALI72,2099-12-31T23:00:00+00:00\n",      # valide → conservée
        encoding="utf-8",
    )

    charger_csv_en_base(engine_missions, csv, "horaires_terminus", colonnes_a_exclure=[])

    with engine_missions.connect() as conn:
        rows = conn.execute(text("SELECT mission FROM horaires_terminus")).fetchall()
    missions = [r[0] for r in rows]
    assert missions == ["KALI72"]
