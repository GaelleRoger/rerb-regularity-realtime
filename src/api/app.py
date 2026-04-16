"""
Interface Streamlit — RER B Régularité temps réel

Affiche trois sections alimentées par l'API FastAPI (http://api:8000) :
  1. Score de régularité : scores par gare clé, positionnés autour du plan
     de la ligne, filtrables par direction (Nord / Sud).
  2. Allongement de parcours : tendance d'accumulation du retard en cours de trajet.
  3. Écarts horaires : retard moyen, médian et maximum des missions en circulation
     par rapport aux horaires théoriques.

La page se rafraîchit automatiquement toutes les 5 minutes via une balise
<meta http-equiv="refresh">, cohérent avec la fréquence du pipeline Airflow.
"""

import os
from datetime import datetime
from pathlib import Path

import requests
import streamlit as st

# ── CONSTANTES ────────────────────────────────────────────────────────────────

# Dossier contenant les assets (logos, plan de ligne) — même dossier que ce script
ASSETS = Path(__file__).parent

# URL de l'API FastAPI (résolution Docker par nom de service)
API_URL = "http://api:8000"

# Couleur officielle de la ligne RER B
RER_B_BLUE = "#0072BC"

# Clé d'API pour l'authentification auprès de l'API FastAPI
_API_KEY = os.environ.get("RERB_API_KEY", "")
_HEADERS = {"X-API-Key": _API_KEY} if _API_KEY else {}

# ── CONFIGURATION DE LA PAGE ──────────────────────────────────────────────────

st.set_page_config(
    page_title="RER B — Régularité temps réel",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Rafraîchissement automatique toutes les 5 minutes (300 secondes)
# La balise meta est interprétée par le navigateur, sans JS ni composant externe.
st.markdown('<meta http-equiv="refresh" content="300">', unsafe_allow_html=True)

# CSS global : réduit les marges par défaut de Streamlit et définit le style
# des en-têtes de section (bandeau bleu RER B).
st.markdown(f"""
<style>
    .block-container {{ padding-top: 3rem; padding-bottom: 1rem; }}
    .section-header {{
        background-color: {RER_B_BLUE};
        color: white;
        padding: 8px 16px;
        border-radius: 6px;
        margin: 16px 0 8px 0;
        font-size: 1.4rem;
        font-weight: bold;
    }}
</style>
""", unsafe_allow_html=True)


# ── FONCTIONS UTILITAIRES ─────────────────────────────────────────────────────

def fetch(route: str):
    """Appelle une route de l'API et retourne le JSON, ou None en cas d'erreur.

    Args:
        route: Chemin de la route sans slash initial (ex: "regularite").

    Returns:
        Données JSON parsées (liste de dicts), ou None si l'API est
        inaccessible ou retourne un statut non-200.
    """
    try:
        r = requests.get(f"{API_URL}/{route}", headers=_HEADERS, timeout=5)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def score_card(label: str, score: int | None) -> str:
    """Génère un bloc HTML coloré affichant un score de régularité.

    Règle de couleur :
      ≥ 85 → vert   (bonne régularité)
      70–84 → orange (régularité dégradée)
      < 70  → rouge  (régularité faible)
      None  → gris   (donnée absente)

    Args:
        label: Nom de la gare ou branche affiché sous le score.
        score: Score entier entre 0 et 100, ou None si indisponible.

    Returns:
        Fragment HTML prêt à passer dans st.markdown(..., unsafe_allow_html=True).
    """
    if score is None:
        color, value = "#adb5bd", "—"
    else:
        value = str(score)
        color = "#28a745" if score >= 85 else ("#fd7e14" if score >= 70 else "#dc3545")
    return f"""
    <div style="background-color:{color};color:white;padding:12px 16px;
                border-radius:10px;text-align:center;margin:4px;">
        <div style="font-size:0.8rem;opacity:0.9;margin-bottom:4px;">{label}</div>
        <div style="font-size:2rem;font-weight:bold;">{value}</div>
    </div>"""


def metric_box(label: str, items: list[tuple[str, str]]) -> str:
    """Génère une boîte HTML affichant une ou plusieurs métriques côte à côte.

    Args:
        label: Titre de la boîte (ex: "Direction Nord — 2026-04-14 08:00").
        items: Liste de tuples (nom_métrique, valeur_formatée).

    Returns:
        Fragment HTML prêt à passer dans st.markdown(..., unsafe_allow_html=True).
    """
    cells = "".join(f"""
        <div style="flex:1;text-align:center;">
            <div style="color:#888;font-size:0.8rem;">{k}</div>
            <div style="color:{RER_B_BLUE};font-size:1.5rem;font-weight:bold;">{v}</div>
        </div>""" for k, v in items)
    return f"""
    <div style="background:white;border:2px solid {RER_B_BLUE};border-radius:8px;
                padding:12px;margin:4px;">
        <div style="color:#555;font-size:0.85rem;margin-bottom:8px;">{label}</div>
        <div style="display:flex;justify-content:space-around;">{cells}</div>
    </div>"""


# ── FETCH ANTICIPÉ ────────────────────────────────────────────────────────────
# Les données de régularité sont récupérées avant le rendu du header afin
# d'afficher l'horodatage réel de la dernière observation en base.
reg_data = fetch("regularite")

# Extraction du max de date_observation toutes directions confondues
heure_maj = "—"
if reg_data:
    dates = [r["date_observation"] for r in reg_data if r.get("date_observation")]
    if dates:
        raw = max(dates)
        try:
            heure_maj = datetime.fromisoformat(raw).strftime("%d/%m/%Y %H:%M")
        except ValueError:
            heure_maj = raw

# ── HEADER ────────────────────────────────────────────────────────────────────
# Disposition en 3 colonnes : logo RER B | titre centré | logo IDF Mobilités
h1, h2, h3 = st.columns([1, 5, 1])
with h1:
    st.image(str(ASSETS / "logo_rerb"), width=100)
with h2:
    st.markdown(
        f"<h1 style='color:{RER_B_BLUE};text-align:center;margin:0;'>"
        "🚆 RER B — Régularité temps réel 🕐</h1>"
        f"<p style='color:#666;text-align:center;margin:4px 0 0 0;font-size:0.95rem;'>"
        f"Dernière mise à jour : {heure_maj}</p>",
        unsafe_allow_html=True,
    )
with h3:
    st.image(str(ASSETS / "IdFMobilités.svg.png"), width=200)

st.divider()

# ── SECTION 1 : SCORE DE RÉGULARITÉ ──────────────────────────────────────────
st.markdown(
    '<div class="section-header">Score de régularité'
    '<div style="font-size:0.85rem;font-weight:normal;opacity:0.85;margin-top:2px;">'
    'Les trains circulent-ils à intervalles réguliers ?</div></div>',
    unsafe_allow_html=True,
)

# Bouton de sélection de direction — Nord sélectionné par défaut (index=0)
radio_col1, radio_col2 = st.columns([1, 4])
with radio_col1:
    st.markdown(
        "<p style='margin:8px 0 0 0;font-size:1rem;'>Dans quelle direction allez-vous ?</p>",
        unsafe_allow_html=True,
    )
with radio_col2:
    direction = st.radio(
        "Direction", ["Nord", "Sud"],
        horizontal=True, index=0, label_visibility="collapsed",
    )

# Récupération des données depuis l'API, puis filtrage sur la direction choisie
reg_data = fetch("regularite")
row = None
if reg_data:
    matches = [r for r in reg_data if r.get("direction") == direction]
    if matches:
        row = matches[0]  # ligne la plus récente pour cette direction


def score(field: str) -> int | None:
    """Extrait un score entier depuis la ligne courante, ou None si absent."""
    return int(row[field]) if row and row.get(field) is not None else None


# Ligne du haut : branches Mitry (gauche) et CDG (droite)
# tc2 est la colonne centrale, laissée vide pour laisser de l'espace au plan
tc1, tc2, tc3 = st.columns([2, 5, 2])
with tc1:
    st.markdown(score_card("Mitry-Claye", score("score_vert_galant")), unsafe_allow_html=True)
with tc3:
    st.markdown(score_card("Saint-Rémy-lès-Ch.", score("score_antony")), unsafe_allow_html=True)

# Plan de la ligne centré, avec marges latérales pour l'alignement visuel
_, img_col, _ = st.columns([1, 8, 1])
with img_col:
    st.image(str(ASSETS / "plan_ligne_b.png"), use_container_width=True)

# Ligne du bas : branches Robinson (gauche), tronçon central Châtelet (centre),
# et Saint-Rémy (droite)
bc1, bc2, bc3 = st.columns([2, 5, 2])
with bc1:
    st.markdown(score_card("Aéroport CDG", score("score_cdg1")), unsafe_allow_html=True)
with bc2:
    st.markdown(score_card("Tronçon central", score("score_chatelet")), unsafe_allow_html=True)
with bc3:
    st.markdown(score_card("Robinson", score("score_sceaux")), unsafe_allow_html=True)

# Horodatage de la dernière observation, ou message d'avertissement si aucune donnée
if row:
    st.caption(f"Dernière observation : {row.get('date_observation', '—')}")
else:
    st.warning(f"Aucune donnée de régularité disponible pour la direction {direction}.")

st.divider()

# ── SECTION 2 : ALLONGEMENT DE PARCOURS ──────────────────────────────────────
# L'allongement mesure si un train accumule du retard en cours de trajet.
# Valeur positive = le retard s'aggrave ; valeur nulle ou négative = stable ou rattrapé.
st.markdown('<div class="section-header">Allongement de parcours'
            '<div style="font-size:0.85rem;font-weight:normal;opacity:0.85;margin-top:2px;">'
            'Mon trajet va t-il prendre plus de temps que prévu ?</div></div>',
            unsafe_allow_html=True)


allong_data = fetch("allongement")
if allong_data:
    # Une colonne par entrée (généralement 2 : Nord et Sud)
    cols = st.columns(len(allong_data))
    for col, entry in zip(cols, allong_data):
        with col:
            val = entry.get("moyenne_allongement")
            val_str = f"{max(0, round(val * 100))}%" if val is not None else "—"
            st.markdown(
                metric_box(
                    f"Direction {entry.get('direction', '—')}",
                    [("Allongement moyen", "+ " + val_str)],
                ),
                unsafe_allow_html=True,
            )
else:
    st.warning("Aucune donnée d'allongement disponible.")

st.divider()

# ── SECTION 3 : ÉCARTS HORAIRES ───────────────────────────────────────────────
# Comparaison entre les horaires réels et théoriques pour les missions en cours.
# ecart_moyen  : retard moyen sur l'ensemble des missions en circulation
# ecart_median : retard médian (moins sensible aux valeurs extrêmes)
# ecart_max    : pire retard observé toutes missions confondues
st.markdown('<div class="section-header">Écarts horaires'
            '<div style="font-size:0.85rem;font-weight:normal;opacity:0.85;margin-top:2px;">'
            'Les horaires théoriques sont-ils respectés ?</div></div>',
            unsafe_allow_html=True)

ecarts_data = fetch("ecarts")
if ecarts_data:
    cols = st.columns(len(ecarts_data))
    for col, entry in zip(cols, ecarts_data):
        with col:
            def fmt(v, dec=1):
                """Formate une valeur en minutes, ou retourne '—' si absente."""
                return f"{v:.{dec}f} min" if v is not None else "—"
            st.markdown(
                metric_box(
                    f"Direction {entry.get('direction', '—')}",
                    [
                        ("Moyen", fmt(entry.get("ecart_moyen"))),
                        ("Médian", fmt(entry.get("ecart_median"))),
                        ("Max", fmt(entry.get("ecart_max"), dec=0)),
                    ],
                ),
                unsafe_allow_html=True,
            )
else:
    st.warning("Aucune donnée d'écarts horaires disponible.")
