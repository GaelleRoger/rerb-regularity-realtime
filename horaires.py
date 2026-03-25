"""
horaires.py
-----------
Récupère les prochains passages RER B à l'arrêt Antony (SP:43066)
via l'API PRIM Île-de-France Mobilités et affiche les résultats
sous forme de DataFrame pandas trié par heure de départ.
"""

import json
from datetime import datetime

import pandas as pd
import requests


# --- Constantes ---------------------------------------------------------------

# URL de l'endpoint stop-monitoring pour l'arrêt Antony (identifiant SP:43066)
API_URL = (
    "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"
    "?MonitoringRef=STIF%3AStopArea%3ASP%3A43066%3A"
)

# En-têtes HTTP requis par l'API PRIM (clé d'authentification + format JSON)
API_HEADERS = {
    "Accept": "application/json",
    "apikey": "GKjpII2hBwd50XKrg78v4uEAhNAZKblh",
}

# Colonnes du DataFrame final et ordre d'affichage
COLONNES = [
    "Train",
    "Destination",
    "Quai",
    "Départ prévu",
    "Départ attendu",
    "Statut départ",
    "À quai",
]


# --- Fonctions ----------------------------------------------------------------

def parse_time(iso_str):
    """
    Convertit une chaîne ISO 8601 UTC en heure locale au format HH:MM.

    Parameters
    ----------
    iso_str : str or None
        Horodatage au format '2026-03-25T08:27:09.000Z', ou None.

    Returns
    -------
    str or None
        Heure locale formatée 'HH:MM', ou None si la valeur est absente.
    """
    if not iso_str:
        return None

    # Remplace le suffixe 'Z' par '+00:00' pour que fromisoformat() l'accepte
    dt_utc = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))

    # Conversion vers le fuseau horaire local de la machine
    return dt_utc.astimezone().strftime("%H:%M")


def extraire_passage(visit):
    """
    Extrait les informations utiles d'un passage (MonitoredStopVisit).

    Parameters
    ----------
    visit : dict
        Un élément de la liste 'MonitoredStopVisit' renvoyée par l'API.

    Returns
    -------
    dict
        Dictionnaire avec les champs définis dans COLONNES.
    """
    journey = visit["MonitoredVehicleJourney"]
    call = journey["MonitoredCall"]

    # Nom du train (ex : 'ERIC47') — premier élément de la liste si présent
    nom_train = (
        journey["VehicleJourneyName"][0]["value"]
        if journey.get("VehicleJourneyName")
        else None
    )

    # Destination affichée en cabine (ex : 'Massy-Palaiseau')
    destination = (
        journey["DestinationName"][0]["value"]
        if journey.get("DestinationName")
        else None
    )

    return {
        "Train": nom_train,
        "Destination": destination,
        # Numéro de quai prévu pour le départ
        "Quai": call.get("DeparturePlatformName", {}).get("value"),
        # Heure théorique (grille horaire) et heure temps réel
        "Départ prévu": parse_time(call.get("AimedDepartureTime")),
        "Départ attendu": parse_time(call.get("ExpectedDepartureTime")),
        # Statut : 'onTime', 'delayed', 'cancelled', etc.
        "Statut départ": call.get("DepartureStatus"),
        # True si le train est déjà à quai au moment de la requête
        "À quai": call.get("VehicleAtStop"),
    }


def construire_dataframe(visits):
    """
    Construit un DataFrame pandas à partir de la liste des passages.

    Parameters
    ----------
    visits : list of dict
        Liste des MonitoredStopVisit renvoyée par l'API.

    Returns
    -------
    pd.DataFrame
        DataFrame trié par heure de départ attendue, colonnes dans l'ordre
        défini par COLONNES.
    """
    rows = [extraire_passage(visit) for visit in visits]

    df = pd.DataFrame(rows, columns=COLONNES)

    # Tri chronologique sur l'heure de départ temps réel
    df = df.sort_values("Départ attendu").reset_index(drop=True)

    return df


def afficher_dataframe(df):
    """
    Configure les options d'affichage pandas et imprime le DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame à afficher.
    """
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)

    print(df.to_string(index=False))


# --- Point d'entrée -----------------------------------------------------------

def main():
    """Orchestre la récupération, la transformation et l'affichage des données."""
    # Appel à l'API PRIM
    reponse = requests.get(API_URL, headers=API_HEADERS)

    if reponse.status_code != 200:
        print(f"Erreur HTTP {reponse.status_code}")
        return

    # Décodage et parsing JSON
    dico = json.loads(reponse.content.decode("utf-8"))

    # Navigation dans la structure SIRI jusqu'à la liste des passages
    visits = (
        dico["Siri"]["ServiceDelivery"]
        ["StopMonitoringDelivery"][0]
        ["MonitoredStopVisit"]
    )

    df = construire_dataframe(visits)
    afficher_dataframe(df)


if __name__ == "__main__":
    main()