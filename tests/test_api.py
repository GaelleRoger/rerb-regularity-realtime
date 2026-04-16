"""
Tests unitaires pour src/api/main.py.

Couvre les 4 routes FastAPI :
  GET /health       → 200 si Postgres joignable, 503 sinon (non protégée)
  GET /regularite   → 200 avec données, 404 si table vide, 401 sans clé
  GET /allongement  → 200 avec données, 404 si table vide, 401 sans clé
  GET /ecarts       → 200 avec données, 404 si table vide, 401 sans clé

La connexion Postgres est simulée par `unittest.mock.patch` :
- `_fetch` est remplacé par un stub retournant des données factices.
- `engine.connect` est remplacé pour simuler les cas succès / échec de /health.
- `_API_KEY` est fixée à "test-key" pour les tests d'authentification.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import api.main as main_module
from api.main import app

# Header valide utilisé dans tous les tests fonctionnels
_VALID_HEADERS = {"X-API-Key": "test-key"}

client = TestClient(app)

# ── Données factices ──────────────────────────────────────────────────────────

_REGULARITE_ROWS = [
    {
        "date_observation": "2026-04-15T08:00:00",
        "direction": "Nord",
        "score_sceaux": 90,
        "score_antony": 85,
        "score_bourg_la_reine": 80,
        "score_chatelet": 78,
        "score_aulnay": 88,
        "score_cdg1": 92,
        "score_vert_galant": 88,
    },
    {
        "date_observation": "2026-04-15T08:00:00",
        "direction": "Sud",
        "score_sceaux": 72,
        "score_antony": 80,
        "score_bourg_la_reine": None,
        "score_chatelet": 65,
        "score_aulnay": None,
        "score_cdg1": 70,
        "score_vert_galant": 75,
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
    """GET /regularite renvoie 200 avec une clé valide."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers=_VALID_HEADERS)

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    directions = {row["direction"] for row in data}
    assert directions == {"Nord", "Sud"}


def test_regularite_contient_scores():
    """Les champs score_* sont bien présents dans la réponse."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers=_VALID_HEADERS)

    row = response.json()[0]
    for champ in ("score_sceaux", "score_antony", "score_chatelet"):
        assert champ in row, f"Champ manquant : {champ}"


def test_regularite_404_si_vide():
    """GET /regularite renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers=_VALID_HEADERS)

    assert response.status_code == 404


def test_regularite_401_sans_cle():
    """GET /regularite renvoie 401 si le header X-API-Key est absent."""
    with patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite")

    assert response.status_code == 401


def test_regularite_401_mauvaise_cle():
    """GET /regularite renvoie 401 si la clé est incorrecte."""
    with patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers={"X-API-Key": "mauvaise-cle"})

    assert response.status_code == 401


# ── /allongement ──────────────────────────────────────────────────────────────

def test_allongement_retourne_donnees():
    """GET /allongement renvoie 200 avec une clé valide."""
    with patch.object(main_module, "_fetch", return_value=_ALLONGEMENT_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/allongement", headers=_VALID_HEADERS)

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_allongement_contient_moyenne():
    """Le champ moyenne_allongement est présent dans la réponse."""
    with patch.object(main_module, "_fetch", return_value=_ALLONGEMENT_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/allongement", headers=_VALID_HEADERS)

    row = response.json()[0]
    assert "moyenne_allongement" in row


def test_allongement_404_si_vide():
    """GET /allongement renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/allongement", headers=_VALID_HEADERS)

    assert response.status_code == 404


def test_allongement_401_sans_cle():
    """GET /allongement renvoie 401 si le header X-API-Key est absent."""
    with patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/allongement")

    assert response.status_code == 401


# ── /ecarts ───────────────────────────────────────────────────────────────────

def test_ecarts_retourne_donnees():
    """GET /ecarts renvoie 200 avec une clé valide."""
    with patch.object(main_module, "_fetch", return_value=_ECARTS_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/ecarts", headers=_VALID_HEADERS)

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_ecarts_contient_metriques():
    """Les champs ecart_moyen, ecart_median, ecart_max sont présents."""
    with patch.object(main_module, "_fetch", return_value=_ECARTS_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/ecarts", headers=_VALID_HEADERS)

    row = response.json()[0]
    for champ in ("ecart_moyen", "ecart_median", "ecart_max"):
        assert champ in row, f"Champ manquant : {champ}"


def test_ecarts_404_si_vide():
    """GET /ecarts renvoie 404 quand la table est vide."""
    with patch.object(main_module, "_fetch", return_value=[]), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/ecarts", headers=_VALID_HEADERS)

    assert response.status_code == 404


def test_ecarts_401_sans_cle():
    """GET /ecarts renvoie 401 si le header X-API-Key est absent."""
    with patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/ecarts")

    assert response.status_code == 401


# ── Validation des modèles Pydantic ──────────────────────────────────────────

def test_regularite_schema_complet():
    """La réponse /regularite contient tous les champs du modèle RegulariteRow."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers=_VALID_HEADERS)

    row = response.json()[0]
    champs_attendus = {
        "date_observation", "direction",
        "score_sceaux", "score_antony", "score_bourg_la_reine",
        "score_chatelet", "score_aulnay", "score_cdg1", "score_vert_galant",
    }
    assert champs_attendus == set(row.keys())


def test_regularite_scores_optionnels_acceptes():
    """Un score None (absent en base) ne doit pas lever d'erreur de validation."""
    with patch.object(main_module, "_fetch", return_value=_REGULARITE_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/regularite", headers=_VALID_HEADERS)

    sud = next(r for r in response.json() if r["direction"] == "Sud")
    assert sud["score_bourg_la_reine"] is None
    assert sud["score_aulnay"] is None


def test_allongement_schema_complet():
    """La réponse /allongement contient tous les champs du modèle AllongementRow."""
    with patch.object(main_module, "_fetch", return_value=_ALLONGEMENT_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/allongement", headers=_VALID_HEADERS)

    row = response.json()[0]
    assert set(row.keys()) == {"date_calcul", "direction", "moyenne_allongement"}


def test_ecarts_schema_complet():
    """La réponse /ecarts contient tous les champs du modèle EcartsRow."""
    with patch.object(main_module, "_fetch", return_value=_ECARTS_ROWS), \
         patch.object(main_module, "_API_KEY", "test-key"):
        response = client.get("/ecarts", headers=_VALID_HEADERS)

    row = response.json()[0]
    assert set(row.keys()) == {"date_observation", "direction", "ecart_moyen", "ecart_median", "ecart_max"}


# ── /health — non protégée ────────────────────────────────────────────────────

def test_health_accessible_sans_cle():
    """/health est accessible sans authentification."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch.object(main_module.engine, "connect", return_value=mock_conn):
        response = client.get("/health")

    assert response.status_code == 200
