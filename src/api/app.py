from pathlib import Path

import requests
import streamlit as st

ASSETS = Path(__file__).parent
API_URL = "http://api:8000"
RER_B_BLUE = "#0072BC"

st.set_page_config(
    page_title="RER B — Régularité temps réel",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Auto-refresh toutes les 5 minutes
st.markdown('<meta http-equiv="refresh" content="300">', unsafe_allow_html=True)

st.markdown(f"""
<style>
    .block-container {{ padding-top: 1rem; padding-bottom: 1rem; }}
    .section-header {{
        background-color: {RER_B_BLUE};
        color: white;
        padding: 8px 16px;
        border-radius: 6px;
        margin: 16px 0 8px 0;
        font-size: 1.1rem;
        font-weight: bold;
    }}
</style>
""", unsafe_allow_html=True)


def fetch(route: str):
    try:
        r = requests.get(f"{API_URL}/{route}", timeout=5)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def score_card(label: str, score: int | None) -> str:
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


# ── HEADER ───────────────────────────────────────────────────────────────────
h1, h2, h3 = st.columns([1, 5, 1])
with h1:
    st.image(str(ASSETS / "logo_rerb"), width=70)
with h2:
    st.markdown(
        f"<h1 style='color:{RER_B_BLUE};text-align:center;margin:0;'>"
        "RER B — Régularité temps réel</h1>",
        unsafe_allow_html=True,
    )
with h3:
    st.image(str(ASSETS / "IdFMobilités.svg.png"), width=120)

st.divider()

# ── RÉGULARITÉ ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">Score de régularité</div>', unsafe_allow_html=True)

direction = st.radio(
    "Direction", ["Nord", "Sud"],
    horizontal=True, index=0, label_visibility="collapsed",
)

reg_data = fetch("regularite")
row = None
if reg_data:
    matches = [r for r in reg_data if r.get("direction") == direction]
    if matches:
        row = matches[0]

def score(field: str) -> int | None:
    return int(row[field]) if row and row.get(field) is not None else None

# Ligne du haut : Mitry (gauche) — CDG (droite)
tc1, tc2, tc3 = st.columns([2, 5, 2])
with tc1:
    st.markdown(score_card("Mitry-Claye", score("score_regularite_vert_galant")), unsafe_allow_html=True)
with tc3:
    st.markdown(score_card("CDG Terminal 1", score("score_regularite_cdg1")), unsafe_allow_html=True)

# Plan de la ligne
_, img_col, _ = st.columns([1, 8, 1])
with img_col:
    st.image(str(ASSETS / "plan_ligne_b.png"), use_container_width=True)

# Ligne du bas : Robinson (gauche) — Châtelet (centre) — Saint-Rémy (droite)
bc1, bc2, bc3 = st.columns([2, 5, 2])
with bc1:
    st.markdown(score_card("Robinson", score("score_regularite_sceaux")), unsafe_allow_html=True)
with bc2:
    st.markdown(score_card("Châtelet-les-Halles", score("score_regularite_chatelet")), unsafe_allow_html=True)
with bc3:
    st.markdown(score_card("Saint-Rémy-lès-Chev.", score("score_regularite_antony")), unsafe_allow_html=True)

if row:
    st.caption(f"Dernière observation : {row.get('date_observation', '—')} — heure : {row.get('heure_th', '—')}h")
else:
    st.warning(f"Aucune donnée de régularité disponible pour la direction {direction}.")

st.divider()

# ── ALLONGEMENT DE PARCOURS ───────────────────────────────────────────────────
st.markdown('<div class="section-header">Allongement de parcours</div>', unsafe_allow_html=True)

allong_data = fetch("allongement")
if allong_data:
    cols = st.columns(len(allong_data))
    for col, entry in zip(cols, allong_data):
        with col:
            val = entry.get("moyenne_allongement")
            val_str = f"{val:.4f}" if val is not None else "—"
            st.markdown(
                metric_box(
                    f"Direction {entry.get('direction', '—')} — {entry.get('date_calcul', '—')}",
                    [("Allongement moyen (min/min)", val_str)],
                ),
                unsafe_allow_html=True,
            )
else:
    st.warning("Aucune donnée d'allongement disponible.")

st.divider()

# ── ÉCARTS HORAIRES ───────────────────────────────────────────────────────────
st.markdown('<div class="section-header">Écarts horaires</div>', unsafe_allow_html=True)

ecarts_data = fetch("ecarts")
if ecarts_data:
    cols = st.columns(len(ecarts_data))
    for col, entry in zip(cols, ecarts_data):
        with col:
            def fmt(v, dec=1):
                return f"{v:.{dec}f} min" if v is not None else "—"
            st.markdown(
                metric_box(
                    f"Direction {entry.get('direction', '—')} — {entry.get('date_observation', '—')}",
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
