"""
Fixtures partagées entre tous les tests unitaires.

- sample_siri_response : réponse JSON IDFM minimale avec 2 missions et 2 arrêts.
- ordre_ids / noms_ids : liste et dictionnaire d'arrêts fictifs pour les tests
  de construire_dataframe().
"""

import pytest


# ── Réponse SIRI factice ──────────────────────────────────────────────────────
# Contient 2 missions (KALI72 vers Saint-Rémy, BIPA84 vers Mitry) et 2 arrêts
# (IDs 100 et 200). Les deux champs AimedDepartureTime et ExpectedDepartureTime
# sont renseignés pour couvrir les tests théoriques et temps réel.
_SAMPLE_SIRI = {
    "Siri": {
        "ServiceDelivery": {
            "EstimatedTimetableDelivery": [{
                "EstimatedJourneyVersionFrame": [{
                    "EstimatedVehicleJourney": [
                        {
                            "VehicleJourneyName": [{"value": "KALI72"}],
                            "DestinationName": [{"value": "Saint-Rémy-lès-Chevreuse"}],
                            "EstimatedCalls": {
                                "EstimatedCall": [
                                    {
                                        "StopPointRef": {"value": "STIF:StopArea:SP:100:"},
                                        "AimedDepartureTime": "2026-04-15T08:00:00+02:00",
                                        "ExpectedDepartureTime": "2026-04-15T08:04:00+02:00",
                                    },
                                    {
                                        "StopPointRef": {"value": "STIF:StopArea:SP:200:"},
                                        "AimedDepartureTime": "2026-04-15T08:10:00+02:00",
                                        "ExpectedDepartureTime": "2026-04-15T08:15:00+02:00",
                                    },
                                ]
                            },
                        },
                        {
                            "VehicleJourneyName": [{"value": "BIPA84"}],
                            "DestinationName": [{"value": "Mitry-Claye"}],
                            "EstimatedCalls": {
                                "EstimatedCall": [
                                    {
                                        "StopPointRef": {"value": "STIF:StopArea:SP:100:"},
                                        "AimedDepartureTime": "2026-04-15T08:20:00+02:00",
                                        "ExpectedDepartureTime": "2026-04-15T08:21:00+02:00",
                                    }
                                ]
                            },
                        },
                    ]
                }]
            }]
        }
    }
}


@pytest.fixture
def sample_siri_response():
    """Réponse SIRI IDFM factice avec 2 missions et 2 arrêts."""
    return _SAMPLE_SIRI


@pytest.fixture
def ordre_ids():
    """Liste ordonnée d'IDs d'arrêts fictifs (100, 200, 300)."""
    return [100, 200, 300]


@pytest.fixture
def noms_ids():
    """Correspondance ID → nom d'arrêt pour les arrêts fictifs."""
    return {100: "gare_a", 200: "gare_b", 300: "gare_c"}
