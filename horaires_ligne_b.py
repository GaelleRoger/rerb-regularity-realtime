import csv
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

TZ_PARIS = ZoneInfo(os.getenv("TZ", "Europe/Paris"))
URL = os.getenv("API_URL", "")
HEADERS: dict[str, str] = {
    "Accept": "application/json",
    "apikey": os.getenv("API_KEY", ""),
}
ARRETS_CSV = os.getenv("ARRETS_CSV", "documents/liste_arrets_ordonnee.csv")


def charger_arrets(chemin_csv: str) -> tuple[list[int], dict[int, str]]:
    """Retourne (liste_ids_ordonnes, dict id_ref_zda -> nom_zda)."""
    ordre: list[int] = []
    noms: dict[int, str] = {}
    with open(chemin_csv, newline="", encoding="utf-8") as f:
        lecteur = csv.DictReader(f)
        for ligne in lecteur:
            id_arret = int(ligne["id_ref_zda"])
            ordre.append(id_arret)
            noms[id_arret] = ligne["nom_zda"]
    return ordre, noms


def extraire_id_arret(stop_point_ref: str) -> int:
    """Extrait l'entier depuis 'STIF:StopArea:SP:43186:'."""
    return int(stop_point_ref.split(":")[-2])


def parser_missions(donnees: dict[str, Any]) -> list[dict[str, Any]]:
    """Retourne une liste de dicts {mission, destination, stop_id: aimed_time}."""
    missions: list[dict[str, Any]] = []
    frames: list[dict[str, Any]] = (
        donnees["Siri"]["ServiceDelivery"]
        ["EstimatedTimetableDelivery"][0]
        ["EstimatedJourneyVersionFrame"]
    )
    for frame in frames:
        for trajet in frame["EstimatedVehicleJourney"]:
            noms_mission: list[dict[str, str]] = trajet.get("VehicleJourneyName", [])
            if not noms_mission:
                continue
            code_mission: str = noms_mission[0]["value"]
            noms_destination: list[dict[str, str]] = trajet.get("DestinationName", [])
            destination: str = noms_destination[0]["value"] if noms_destination else ""

            appels: list[dict[str, Any]] = (
                trajet.get("EstimatedCalls", {}).get("EstimatedCall", [])
            )
            horaires: dict[int, datetime] = {}
            for appel in appels:
                aimed: str | None = appel.get("AimedDepartureTime")
                if aimed is None:
                    continue
                id_arret: int = extraire_id_arret(appel["StopPointRef"]["value"])
                horaires[id_arret] = datetime.fromisoformat(
                    aimed.replace("Z", "+00:00")
                ).astimezone(TZ_PARIS)

            missions.append({
                "mission": code_mission,
                "destination": destination,
                **horaires,
            })
    return missions


def construire_dataframe(
    missions: list[dict[str, Any]],
    ordre_ids: list[int],
    noms_ids: dict[int, str],
) -> pd.DataFrame:
    """Construit le DataFrame avec les colonnes ordonnées selon le CSV."""
    date_observation: datetime = datetime.now(tz=TZ_PARIS)

    df = pd.DataFrame(missions)

    # Colonnes arrêts dans l'ordre du CSV (seulement ceux présents dans les données)
    colonnes_arrets: list[int] = [i for i in ordre_ids if i in df.columns]
    df = df[["mission", "destination"] + colonnes_arrets]
    df.insert(1, "type_mission", df["mission"].str[:4])

    # Renommer les colonnes numériques par le nom de l'arrêt
    df = df.rename(columns=noms_ids)

    noms_arrets: list[str] = [noms_ids[i] for i in colonnes_arrets]
    df["gare_depart"] = df[noms_arrets].apply(
        lambda row: row.idxmin() if row.notna().any() else None, axis=1
    )
    df["nb_arrets_desservis"] = df[noms_arrets].notna().sum(axis=1)

    df["date_observation"] = date_observation
    return df


def exporter_csv(df: pd.DataFrame) -> None:
    """Exporte le DataFrame dans un fichier CSV horodaté."""
    horodatage: str = datetime.now().strftime("%Y%m%d_%H%M%S")
    nom_fichier: str = f"horaires_theoriques_{horodatage}.csv"
    df.to_csv(nom_fichier, index=False)
    print(f"Exporté : {nom_fichier}")


def main() -> None:
    ordre_ids, noms_ids = charger_arrets(ARRETS_CSV)

    reponse = requests.get(URL, headers=HEADERS, timeout=30)
    reponse.raise_for_status()

    donnees: dict[str, Any] = reponse.json()
    missions = parser_missions(donnees)

    df = construire_dataframe(missions, ordre_ids, noms_ids)
    print(df.to_string())
    exporter_csv(df)


if __name__ == "__main__":
    main()
