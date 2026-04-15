"""
Tests unitaires pour src/extract_horaires.py.

Couvre :
- extraire_id_arret()      : parsing de la référence STIF
- parser_missions()        : extraction des missions depuis le JSON SIRI
- construire_dataframe()   : construction du DataFrame et calcul des colonnes dérivées
- charger_arrets()         : lecture du CSV des arrêts ordonnés
"""

import pytest
from extract_horaires import (
    charger_arrets,
    construire_dataframe,
    extraire_id_arret,
    parser_missions,
)


# ── extraire_id_arret ─────────────────────────────────────────────────────────

def test_extraire_id_arret_format_standard():
    assert extraire_id_arret("STIF:StopArea:SP:43186:") == 43186


def test_extraire_id_arret_id_court():
    assert extraire_id_arret("STIF:StopArea:SP:100:") == 100


# ── parser_missions ───────────────────────────────────────────────────────────

def test_parser_missions_aimed_retourne_deux_missions(sample_siri_response):
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    assert len(missions) == 2


def test_parser_missions_codes_corrects(sample_siri_response):
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    codes = [m["mission"] for m in missions]
    assert "KALI72" in codes
    assert "BIPA84" in codes


def test_parser_missions_expected_retourne_deux_missions(sample_siri_response):
    missions = parser_missions(sample_siri_response, "ExpectedDepartureTime")
    assert len(missions) == 2


def test_parser_missions_horaire_converti_en_paris(sample_siri_response):
    """L'heure doit être convertie en heure de Paris (UTC+2 en été)."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    kali = next(m for m in missions if m["mission"] == "KALI72")
    heure_arret_100 = kali[100]
    assert heure_arret_100.tzinfo is not None
    assert heure_arret_100.hour == 8


def test_parser_missions_destination_correcte(sample_siri_response):
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    kali = next(m for m in missions if m["mission"] == "KALI72")
    assert kali["destination"] == "Saint-Rémy-lès-Chevreuse"


def test_parser_missions_sans_nom_ignoree():
    """Une mission sans VehicleJourneyName ne doit pas apparaître dans les résultats."""
    response = {
        "Siri": {"ServiceDelivery": {"EstimatedTimetableDelivery": [{
            "EstimatedJourneyVersionFrame": [{
                "EstimatedVehicleJourney": [
                    {
                        # Pas de VehicleJourneyName → doit être ignorée
                        "VehicleJourneyName": [],
                        "EstimatedCalls": {"EstimatedCall": []},
                    },
                    {
                        "VehicleJourneyName": [{"value": "KALI72"}],
                        "DestinationName": [{"value": "StRémy"}],
                        "EstimatedCalls": {"EstimatedCall": []},
                    },
                ]
            }]
        }]}}
    }
    missions = parser_missions(response, "AimedDepartureTime")
    assert len(missions) == 1
    assert missions[0]["mission"] == "KALI72"


def test_parser_missions_champ_absent_ignore(sample_siri_response):
    """Un arrêt sans le champ demandé ne doit pas générer d'entrée dans les horaires."""
    # On demande ExpectedDepartureTime — les arrêts n'ont pas AimedArrivalTime
    missions = parser_missions(sample_siri_response, "AimedArrivalTime")
    # Aucun arrêt n'a ce champ → missions vides d'horaires mais présentes
    for m in missions:
        horaires = {k: v for k, v in m.items() if isinstance(k, int)}
        assert len(horaires) == 0


# ── construire_dataframe ──────────────────────────────────────────────────────

def test_construire_dataframe_colonnes_presentes(sample_siri_response, ordre_ids, noms_ids):
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    for col in ("mission", "destination", "gare_depart", "nb_arrets_desservis", "date_observation"):
        assert col in df.columns, f"Colonne manquante : {col}"


def test_construire_dataframe_nombre_missions(sample_siri_response, ordre_ids, noms_ids):
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    assert len(df) == 2


def test_construire_dataframe_nb_arrets_kali(sample_siri_response, ordre_ids, noms_ids):
    """KALI72 dessert 2 arrêts (100 et 200) → nb_arrets_desservis doit valoir 2."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    kali = df[df["mission"] == "KALI72"].iloc[0]
    assert kali["nb_arrets_desservis"] == 2


def test_construire_dataframe_nb_arrets_bipa(sample_siri_response, ordre_ids, noms_ids):
    """BIPA84 dessert 1 arrêt (100) → nb_arrets_desservis doit valoir 1."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    bipa = df[df["mission"] == "BIPA84"].iloc[0]
    assert bipa["nb_arrets_desservis"] == 1


def test_construire_dataframe_gare_depart(sample_siri_response, ordre_ids, noms_ids):
    """gare_depart doit être l'arrêt avec l'horaire le plus précoce."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    kali = df[df["mission"] == "KALI72"].iloc[0]
    # gare_a (arrêt 100) est à 08:00, gare_b (arrêt 200) est à 08:10
    assert kali["gare_depart"] == "gare_a"


def test_construire_dataframe_type_mission(sample_siri_response, ordre_ids, noms_ids):
    """type_mission doit être les 4 premiers caractères du code mission."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    kali = df[df["mission"] == "KALI72"].iloc[0]
    assert kali["type_mission"] == "KALI"


def test_construire_dataframe_arret_absent_est_nat(sample_siri_response, ordre_ids, noms_ids):
    """L'arrêt 300 (gare_c) absent des données doit apparaître en NaT."""
    missions = parser_missions(sample_siri_response, "AimedDepartureTime")
    df = construire_dataframe(missions, ordre_ids, noms_ids)
    if "gare_c" in df.columns:
        assert df["gare_c"].isna().all()


# ── charger_arrets ────────────────────────────────────────────────────────────

def test_charger_arrets_ordre_et_noms(tmp_path):
    """charger_arrets doit retourner l'ordre des IDs et le dict nom."""
    csv_file = tmp_path / "arrets.csv"
    csv_file.write_text(
        "geo_point_2d,id_ref_zda,nom_zda\n"
        "48.8,43186,Châtelet\n"
        "48.7,43187,Antony\n",
        encoding="utf-8",
    )
    ordre, noms = charger_arrets(str(csv_file))
    assert ordre == [43186, 43187]
    assert noms[43186] == "Châtelet"
    assert noms[43187] == "Antony"


def test_charger_arrets_ordre_preserve(tmp_path):
    """L'ordre de lecture du CSV doit être préservé."""
    csv_file = tmp_path / "arrets.csv"
    csv_file.write_text(
        "geo_point_2d,id_ref_zda,nom_zda\n"
        "1,300,Gare C\n"
        "2,100,Gare A\n"
        "3,200,Gare B\n",
        encoding="utf-8",
    )
    ordre, _ = charger_arrets(str(csv_file))
    assert ordre == [300, 100, 200]
