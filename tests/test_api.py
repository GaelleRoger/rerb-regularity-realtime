"""
Tests unitaires pour src/api/main.py.

Couvre les 4 routes FastAPI :
  GET /health       → 200 si Postgres joignable, 503 sinon
  GET /regularite   → 200 avec données, 404 si table vide
  GET /allongement  → 200 avec données, 404 si table vide
  GET /ecarts       → 200 avec données, 404 si table vide

La connexion Postgres est simulée par `unittest.mock.patch` :
- `_fetch` est remplacé par un stub retournant des données factices.
- `engine.connect` est remplacé pour simuler les cas succès / échec de /health.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import api.main as main_module
from api.main import app

client = TestClient(app)

# ── Données factices ──────────────────────────────────────────────────────────

_REGULARITE_ROWS = [
    {
        "date_observation": "2026-04-15T08:00:00",
        "direction": "Nord",
        "score_regularite_sceaux": 90,
        "score_regularite_antony": 85,
        "score_regularite_chatelet": 78,
        "score_regularite_cdg1": 92,
        "score_regularite_vert_galant": 88,
    },
    {
        "date_observation": "2026-04-15T08:00:00",
        "direction": "Sud",
        "score_regularite_sceaux": 72,
        "score_regularite_antony": 80,
        "score_regularite_chatelet": 65,
        "score_regularite_cdg1": 70,
        "score_regularite_vert_galant": 75,
    },
]

_ALLONGEMENT_ROWS = [
    {"date_calcul": "2026-04-15T08:00:00", "direction": "Nord", "moyenne_allongement": 0.12},
    {"date_calcul": "2026-04-15T08:00:00", "direction": "Sud", "moyenne_allongement": 0.05},
]

_ECARTS_ROWS = [
    {"date_observation": "2026-04-15T08:00:00", "direction": "Nord",
     "ecart_moyen": 2.5, "ecart_median": 2.0, "ecart_max": 8.0},
    {"date_observation": "2026-04-15T08:00:00", "direction": "Sud",
     "ecart_moyen": 1.2, "ecart_median": 1.0, "ecart_max": 4.0},
]


# ── /health ───────────────────────────────────────────────────────────────────

def test_health_ok():
    """GET /health renvoie 200 quand SELECT 1 réussit."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch.object(main_module.engine, "connect", return_value=mock_conn):
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_postgres_indisponible():
    """GET /health renvoie 503 si la connexion échoue."""
    with patch.object(main_module.engine, "connect", side_effect=Exception("connexion refusée")):
        response = client.get("/health")

    assert response.status_code == 503


# ── /regularite ───────────────────────────────────────────────────────────────

def test_regularite_retourne_donnees():
    """GET /regularite renvoie 200 et les lignes de régularité."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS):
        response = client.get("/regularite")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    directions = {row["direction"] for row in data}
    assert directions == {"Nord", "Sud"}


def test_regularite_contient_scores():
    """Les champs score_regularite_* sont bien présents dans la réponse."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS):
        response = client.get("/regularite")

    row = response.json()[0]
    for champ in ("score_regularite_sceaux", "score_regularite_antony", "score_regularite_chatelet"):
        assert champ in row, f"Champ manquant : {champ}"


def test_regularite_404_si_vide():
    """GET /regularite renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]):
        response = client.get("/regularite")

    assert response.status_code == 404


# ── /allongement ──────────────────────────────────────────────────────────────

def test_allongement_retourne_donnees():
    """GET /allongement renvoie 200 et les lignes d'allongement."""
    with patch.object(main_module, "_fetch", return_value=_ALLONGEMENT_ROWS):
        response = client.get("/allongement")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_allongement_contient_moyenne():
    """Le champ moyenne_allongement est présent dans la réponse."""
    with patch.object(main_module, "_fetch", return_value=_ALLONGEMENT_ROWS):
        response = client.get("/allongement")

    row = response.json()[0]
    assert "moyenne_allongement" in row


def test_allongement_404_si_vide():
    """GET /allongement renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]):
        response = client.get("/allongement")

    assert response.status_code == 404


# ── /ecarts ───────────────────────────────────────────────────────────────────

def test_ecarts_retourne_donnees():
    """GET /ecarts renvoie 200 et les lignes d'écarts horaires."""
    with patch.object(main_module, "_fetch", return_value=_ECARTS_ROWS):
        response = client.get("/ecarts")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_ecarts_contient_metriques():
    """Les champs ecart_moyen, ecart_median, ecart_max sont présents."""
    with patch.object(main_module, "_fetch", return_value=_ECARTS_ROWS):
        response = client.get("/ecarts")

    row = response.json()[0]
    for champ in ("ecart_moyen", "ecart_median", "ecart_max"):
        assert champ in row, f"Champ manquant : {champ}"


def test_ecarts_404_si_vide():
    """GET /ecarts renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]):
        response = client.get("/ecarts")

    assert response.status_code == 404
