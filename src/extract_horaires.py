import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TZ_PARIS = ZoneInfo(os.getenv("TZ", "Europe/Paris"))
URL = os.getenv("API_URL", "")
HEADERS: dict[str, str] = {
    "Accept": "application/json",
    "apikey": os.getenv("API_KEY", ""),
}
ARRETS_CSV = str(RACINE / "documents/liste_arrets_ordonnee.csv")


def charger_arrets(chemin_csv: str) -> tuple[list[int], dict[int, str]]:
    """Charge la liste ordonnée des arrêts depuis un fichier CSV.

    Args:
        chemin_csv: Chemin vers le fichier CSV contenant les colonnes
            geo_point_2d, id_ref_zda et nom_zda.

    Returns:
        Un tuple (ordre, noms) où :
        - ordre est la liste des id_ref_zda dans l'ordre géographique du CSV.
        - noms est un dict associant chaque id_ref_zda à son nom d'arrêt.
    """
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
    """Extrait l'identifiant numérique d'un arrêt depuis sa référence STIF.
    Permet la correspondance entre les résultats API et le nom de l'arrêt

    Args:
        stop_point_ref: Référence STIF au format 'STIF:StopArea:SP:43186:'.

    Returns:
        L'identifiant entier de l'arrêt (ex: 43186).
    """
    return int(stop_point_ref.split(":")[-2])


def parser_missions(
    donnees: dict[str, Any],
    champ_heure: str,
) -> list[dict[str, Any]]:
    """Extrait les missions et leurs horaires depuis la réponse API.

    Parcourt l'arborescence SIRI EstimatedTimetable et retourne pour chaque
    trajet son code mission, sa destination et l'heure de départ au champ
    demandé, convertie en heure de Paris.

    Args:
        donnees: JSON retourné par l'API IDFM requete-ligne.
        champ_heure: Champ de temps à extraire dans chaque EstimatedCall.
            Utiliser 'AimedDepartureTime' pour les horaires théoriques ou
            'ExpectedDepartureTime' pour les horaires temps réel.

    Returns:
        Liste de dicts, un par trajet, contenant :
        - 'mission' (str) : code complet de la mission (ex: 'KALI72').
        - 'destination' (str) : nom de la gare terminus.
        - id_arret (int) -> datetime : heure de départ par arrêt en heure de
          Paris. NaT si le champ est absent pour cet arrêt.
    """
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
                heure: str | None = appel.get(champ_heure)
                if heure is None:
                    continue
                id_arret: int = extraire_id_arret(appel["StopPointRef"]["value"])
                horaires[id_arret] = datetime.fromisoformat(
                    heure.replace("Z", "+00:00")
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
    """Construit le DataFrame des horaires théoriques par mission et par arrêt.

    Les colonnes sont ordonnées selon l'ordre géographique du CSV des arrêts.
    Les arrêts non desservis par une mission apparaissent en NaT.

    Args:
        missions: Liste de dicts produite par parser_missions().
        ordre_ids: Liste des id_ref_zda dans l'ordre géographique.
        noms_ids: Dict associant chaque id_ref_zda à son nom d'arrêt.

    Returns:
        DataFrame avec une ligne par mission et les colonnes suivantes :
        - mission, type_mission, destination : identifiants de la mission.
        - [nom_arret] x47 : horaire de départ théorique (ou NaT si la gare en question n'est pas desservie).
        - gare_depart : nom de l'arrêt avec l'horaire le plus précoce.
        - nb_arrets_desservis : nombre d'arrêts avec un horaire renseigné.
          (permet ensuite de déterminer si un train est en circulation ou non)
        - date_observation : horodatage de création du DataFrame.
    """
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


DOSSIER_EXPORT = RACINE / "data/raw"


def exporter_csv(df: pd.DataFrame, prefixe: str) -> None:
    """Exporte le DataFrame dans un fichier CSV horodaté.

    Le nom du fichier suit le format {prefixe}_YYYYMMDD_HHMMSS.csv
    et est créé dans le dossier data/raw/.

    Args:
        df: DataFrame produit par construire_dataframe().
        prefixe: Préfixe du nom de fichier (ex: 'horaires_theoriques'
            ou 'horaires_reels').
    """
    horodatage: str = datetime.now().strftime("%Y%m%d_%H%M%S")
    chemin: Path = DOSSIER_EXPORT / f"{prefixe}_{horodatage}.csv"
    df.to_csv(chemin, index=False)
    print(f"Exporté : {chemin}")


def main() -> None:
    """Point d'entrée du script.

    Charge les arrêts, interroge l'API IDFM, construit les DataFrames des
    horaires théoriques et temps réel depuis le même appel API, et exporte
    chacun dans un CSV horodaté distinct.
    """
    ordre_ids, noms_ids = charger_arrets(ARRETS_CSV)

    reponse = requests.get(URL, headers=HEADERS, timeout=30)
    reponse.raise_for_status()

    donnees: dict[str, Any] = reponse.json()

    missions_theoriques = parser_missions(donnees, "AimedDepartureTime")
    df_theoriques = construire_dataframe(missions_theoriques, ordre_ids, noms_ids)
    print("=== Horaires théoriques ===")
    #print(df_theoriques.to_string())
    exporter_csv(df_theoriques, "horaires_theoriques")

    missions_reels = parser_missions(donnees, "ExpectedDepartureTime")
    df_reels = construire_dataframe(missions_reels, ordre_ids, noms_ids)
    print("\n=== Horaires temps réel ===")
    #print(df_reels.to_string())
    exporter_csv(df_reels, "horaires_reels")


if __name__ == "__main__":
    main()
