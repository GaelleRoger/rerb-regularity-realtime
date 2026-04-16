"""
Tests d'intégration — validation du schéma et de l'intégrité des tables calculées.

Ces tests nécessitent une base Postgres active avec les tables déjà peuplées
par le pipeline Airflow. Ils sont ignorés par défaut (voir pytest.ini).
Pour les exécuter : pytest -m integration

Couvre :
- Présence des tables hist_moyenne_regularite, hist_moyenne_allongement, hist_moyenne_ecarts
- Colonnes attendues dans chaque table
- Contraintes métier (scores entre 0 et 100, direction dans {Nord, Sud})
"""

import pytest
from sqlalchemy import inspect, text

pytestmark = pytest.mark.integration


# ── Helpers ───────────────────────────────────────────────────────────────────

def colonnes_table(engine, table: str) -> set[str]:
    """Retourne l'ensemble des noms de colonnes d'une table Postgres."""
    inspector = inspect(engine)
    return {col["name"] for col in inspector.get_columns(table)}


def table_existe(engine, table: str) -> bool:
    """Retourne True si la table existe dans le schéma public."""
    inspector = inspect(engine)
    return table in inspector.get_table_names(schema="public")


# ── hist_moyenne_regularite ───────────────────────────────────────────────────

def test_table_regularite_existe(pg_engine):
    assert table_existe(pg_engine, "hist_moyenne_regularite"), \
        "La table hist_moyenne_regularite est absente"


def test_colonnes_regularite(pg_engine):
    attendues = {
        "date_observation", "direction",
        "score_sceaux", "score_antony", "score_bourg_la_reine",
        "score_chatelet", "score_aulnay", "score_cdg1", "score_vert_galant",
    }
    presentes = colonnes_table(pg_engine, "hist_moyenne_regularite")
    manquantes = attendues - presentes
    assert not manquantes, f"Colonnes manquantes dans hist_moyenne_regularite : {manquantes}"


def test_direction_regularite_valeurs(pg_engine):
    """La colonne direction ne doit contenir que 'Nord' ou 'Sud'."""
    with pg_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT direction FROM hist_moyenne_regularite")
        ).fetchall()
    directions = {r[0] for r in rows}
    assert directions.issubset({"Nord", "Sud"}), \
        f"Valeurs inattendues dans direction : {directions - {'Nord', 'Sud'}}"


def test_scores_regularite_dans_plage(pg_engine):
    """Tous les scores de régularité doivent être compris entre 0 et 100."""
    colonnes_scores = [
        "score_sceaux", "score_antony", "score_bourg_la_reine",
        "score_chatelet", "score_aulnay", "score_cdg1", "score_vert_galant",
    ]
    for col in colonnes_scores:
        with pg_engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT MIN({col}), MAX({col}) FROM hist_moyenne_regularite")
            ).fetchone()
        if row and row[0] is not None:
            assert row[0] >= 0, f"{col} : valeur min négative ({row[0]})"
            assert row[1] <= 100, f"{col} : valeur max > 100 ({row[1]})"


# ── hist_moyenne_allongement ──────────────────────────────────────────────────

def test_table_allongement_existe(pg_engine):
    assert table_existe(pg_engine, "hist_moyenne_allongement"), \
        "La table hist_moyenne_allongement est absente"


def test_colonnes_allongement(pg_engine):
    attendues = {"date_calcul", "direction", "moyenne_allongement"}
    presentes = colonnes_table(pg_engine, "hist_moyenne_allongement")
    manquantes = attendues - presentes
    assert not manquantes, f"Colonnes manquantes dans hist_moyenne_allongement : {manquantes}"


def test_direction_allongement_valeurs(pg_engine):
    with pg_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT direction FROM hist_moyenne_allongement")
        ).fetchall()
    directions = {r[0] for r in rows}
    assert directions.issubset({"Nord", "Sud"}), \
        f"Valeurs inattendues dans direction : {directions - {'Nord', 'Sud'}}"


# ── hist_moyenne_ecarts ───────────────────────────────────────────────────────

def test_table_ecarts_existe(pg_engine):
    assert table_existe(pg_engine, "hist_moyenne_ecarts"), \
        "La table hist_moyenne_ecarts est absente"


def test_colonnes_ecarts(pg_engine):
    attendues = {"date_observation", "direction", "ecart_moyen", "ecart_median", "ecart_max"}
    presentes = colonnes_table(pg_engine, "hist_moyenne_ecarts")
    manquantes = attendues - presentes
    assert not manquantes, f"Colonnes manquantes dans hist_moyenne_ecarts : {manquantes}"


def test_ecart_max_superieur_a_moyen(pg_engine):
    """ecart_max doit être >= ecart_moyen pour chaque ligne."""
    with pg_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT ecart_moyen, ecart_max
                FROM hist_moyenne_ecarts
                WHERE ecart_moyen IS NOT NULL AND ecart_max IS NOT NULL
            """)
        ).fetchall()
    for moyen, maxi in rows:
        assert maxi >= moyen, \
            f"ecart_max ({maxi}) < ecart_moyen ({moyen}) — incohérence détectée"
