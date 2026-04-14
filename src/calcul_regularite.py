from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connexion_postgres import creer_engine

RACINE = Path(__file__).parent.parent

load_dotenv(RACINE / ".env")

TABLE_CIBLE = "regularite_theorique"

SQL_RECONSTRUCTION = f"""
    DROP TABLE IF EXISTS {TABLE_CIBLE};

    CREATE TABLE {TABLE_CIBLE} AS
    WITH init AS (
    SELECT
        date_observation,
        MIN(date_observation) OVER (PARTITION BY mission) AS date_min,
        mission, sceaux, antony, bourg_la_reine, chatelet_les_halles,
        aulnay_sous_bois, aeroport_cdg_1_rer, vert_galant,
        (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q','N','M','G') THEN 'Nord' ELSE 'Sud' END) AS direction
    FROM horaires_theoriques_trv
),
delta AS (
    SELECT
        date_min, mission, direction,
        EXTRACT(hour FROM sceaux::timestamp)              AS heure_sceaux,
        LAG(sceaux) OVER (PARTITION BY direction ORDER BY sceaux)                          AS sceaux_precedent, sceaux,
        EXTRACT(hour FROM antony::timestamp)              AS heure_antony,
        LAG(antony) OVER (PARTITION BY direction ORDER BY antony)                          AS antony_precedent, antony,
        EXTRACT(hour FROM bourg_la_reine::timestamp)      AS heure_bourg_la_reine,
        LAG(bourg_la_reine) OVER (PARTITION BY direction ORDER BY bourg_la_reine)          AS bourg_la_reine_precedent, bourg_la_reine,
        EXTRACT(hour FROM chatelet_les_halles::timestamp) AS heure_chatelet,
        LAG(chatelet_les_halles) OVER (PARTITION BY direction ORDER BY chatelet_les_halles) AS chatelet_precedent, chatelet_les_halles,
        EXTRACT(hour FROM aulnay_sous_bois::timestamp)    AS heure_aulnay,
        LAG(aulnay_sous_bois) OVER (PARTITION BY direction ORDER BY aulnay_sous_bois)      AS aulnay_precedent, aulnay_sous_bois,
        EXTRACT(hour FROM aeroport_cdg_1_rer::timestamp)  AS heure_cdg1,
        LAG(aeroport_cdg_1_rer) OVER (PARTITION BY direction ORDER BY aeroport_cdg_1_rer)  AS cdg1_precedent, aeroport_cdg_1_rer,
        EXTRACT(hour FROM vert_galant::timestamp)         AS heure_vert_galant,
        LAG(vert_galant) OVER (PARTITION BY direction ORDER BY vert_galant)                AS vert_galant_precedent, vert_galant
    FROM init
    WHERE date_observation = date_min
    ORDER BY direction, chatelet_les_halles
)
SELECT
    mission,
    direction,
    heure_sceaux             AS heure_th_sceaux,
    ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER             AS delta_sceaux,
    heure_antony             AS heure_th_antony,
    ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER             AS delta_antony,
    heure_bourg_la_reine     AS heure_th_bourg_la_reine,
    ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine,
    heure_chatelet           AS heure_th_chatelet,
    ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER  AS delta_chatelet,
    heure_aulnay             AS heure_th_aulnay,
    ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER   AS delta_aulnay,
    heure_cdg1               AS heure_th_cdg1,
    ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER   AS delta_cdg1,
    heure_vert_galant        AS heure_th_vert_galant,
    ROUND(EXTRACT(EPOCH FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) / 60)::INTEGER   AS delta_vert_galant
FROM delta;
"""


TABLE_CIBLE_REELLE = "regularite_reelle"

SQL_RECONSTRUCTION_REELLE = f"""
    DROP TABLE IF EXISTS {TABLE_CIBLE_REELLE};

    CREATE TABLE {TABLE_CIBLE_REELLE} AS
    WITH init as (SELECT 
        mission, (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q','N','M','G') THEN 'Nord' ELSE 'Sud' END) AS direction,
        MAX(sceaux) as sceaux, MAX(antony) as antony,
        MAX(bourg_la_reine) as bourg_la_reine, MAX(chatelet_les_halles) as chatelet_les_halles,
        MAX(aulnay_sous_bois) as aulnay_sous_bois, 
        MAX(aeroport_cdg_1_rer) as aeroport_cdg_1_rer, MAX(vert_galant) as vert_galant
    FROM horaires_reels_trv
    GROUP BY 1,2
    ORDER BY direction, chatelet_les_halles
),

delta_heure AS (
    SELECT 
        mission, direction,
        EXTRACT(hour FROM sceaux::timestamp)             AS heure_sceaux,
        LAG(sceaux) OVER (PARTITION BY direction ORDER BY sceaux)                       AS sceaux_precedent, sceaux,
        EXTRACT(hour FROM antony::timestamp)             AS heure_antony,
        LAG(antony) OVER (PARTITION BY direction ORDER BY antony)                       AS antony_precedent, antony,
        EXTRACT(hour FROM bourg_la_reine::timestamp)     AS heure_bourg_la_reine,
        LAG(bourg_la_reine) OVER (PARTITION BY direction ORDER BY bourg_la_reine)       AS bourg_la_reine_precedent, bourg_la_reine,
        EXTRACT(hour FROM chatelet_les_halles::timestamp) AS heure_chatelet,
        LAG(chatelet_les_halles) OVER (PARTITION BY direction ORDER BY chatelet_les_halles) AS chatelet_precedent, chatelet_les_halles,
        EXTRACT(hour FROM aulnay_sous_bois::timestamp)   AS heure_aulnay,
        LAG(aulnay_sous_bois) OVER (PARTITION BY direction ORDER BY aulnay_sous_bois)   AS aulnay_precedent, aulnay_sous_bois,
        EXTRACT(hour FROM aeroport_cdg_1_rer::timestamp) AS heure_cdg1,
        LAG(aeroport_cdg_1_rer) OVER (PARTITION BY direction ORDER BY aeroport_cdg_1_rer) AS cdg1_precedent, aeroport_cdg_1_rer,
        EXTRACT(hour FROM vert_galant::timestamp)        AS heure_vert_galant,
        LAG(vert_galant) OVER (PARTITION BY direction ORDER BY vert_galant)             AS vert_galant_precedent, vert_galant
    FROM init
    ORDER BY direction, chatelet_les_halles
)
SELECT
    mission,
    direction,
    heure_sceaux             AS heure_re_sceaux,
    ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER             AS delta_sceaux,
    heure_antony             AS heure_re_antony,
    ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER             AS delta_antony,
    heure_bourg_la_reine     AS heure_re_bourg_la_reine,
    ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine,
    heure_chatelet           AS heure_re_chatelet,
    ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER  AS delta_chatelet,
    heure_aulnay             AS heure_re_aulnay,
    ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER   AS delta_aulnay,
    heure_cdg1               AS heure_re_cdg1,
    ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER   AS delta_cdg1,
    heure_vert_galant        AS heure_re_vert_galant,
    ROUND(EXTRACT(EPOCH FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) / 60)::INTEGER   AS delta_vert_galant
FROM delta_heure;
"""


TABLE_HISTO_REG = "hist_moyenne_regularite"

SQL_CREATION_HISTO_REG = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_HISTO_REG} (
        date_observation                TIMESTAMP,
        heure_th                        NUMERIC,
        direction                       TEXT,
        score_regularite_sceaux         INTEGER,
        score_regularite_antony         INTEGER,
        score_regularite_bourg_la_reine INTEGER,
        score_regularite_chatelet       INTEGER,
        score_regularite_aulnay         INTEGER,
        score_regularite_cdg1           INTEGER,
        score_regularite_vert_galant    INTEGER
    )
"""


SQL_INSERTION_HISTO_REG = f"""
    INSERT INTO {TABLE_HISTO_REG}
-- ============================================================
-- Régularité par tranche horaire et direction — toutes gares
-- Format large : une colonne score par gare
-- Filtre : heure théorique = heure courante (Europe/Paris)
-- ============================================================

WITH heure_courante AS (
  SELECT EXTRACT(HOUR FROM NOW() AT TIME ZONE 'Europe/Paris') AS h
),

-- ── SCEAUX ──────────────────────────────────────────────────
penalites_sceaux AS (
  SELECT
    th.heure_th_sceaux        AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_sceaux - th.delta_sceaux)::numeric / th.delta_sceaux
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_sceaux  IS NOT NULL
    AND re.delta_sceaux  IS NOT NULL
    AND th.delta_sceaux  > 0
    AND th.heure_th_sceaux = (SELECT h FROM heure_courante)
),
score_sceaux AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_sceaux
  FROM penalites_sceaux
  GROUP BY heure_th, direction
),

-- ── ANTONY ──────────────────────────────────────────────────
penalites_antony AS (
  SELECT
    th.heure_th_antony         AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_antony - th.delta_antony)::numeric / th.delta_antony
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_antony  IS NOT NULL
    AND re.delta_antony  IS NOT NULL
    AND th.delta_antony  > 0
    AND th.heure_th_antony = (SELECT h FROM heure_courante)
),
score_antony AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_antony
  FROM penalites_antony
  GROUP BY heure_th, direction
),

-- ── BOURG-LA-REINE ──────────────────────────────────────────
penalites_bourg_la_reine AS (
  SELECT
    th.heure_th_bourg_la_reine   AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_bourg_la_reine - th.delta_bourg_la_reine)::numeric / th.delta_bourg_la_reine
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_bourg_la_reine  IS NOT NULL
    AND re.delta_bourg_la_reine  IS NOT NULL
    AND th.delta_bourg_la_reine  > 0
    AND th.heure_th_bourg_la_reine = (SELECT h FROM heure_courante)
),
score_bourg_la_reine AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_bourg_la_reine
  FROM penalites_bourg_la_reine
  GROUP BY heure_th, direction
),

-- ── CHATELET ────────────────────────────────────────────────
penalites_chatelet AS (
  SELECT
    th.heure_th_chatelet       AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_chatelet - th.delta_chatelet)::numeric / th.delta_chatelet
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_chatelet  IS NOT NULL
    AND re.delta_chatelet  IS NOT NULL
    AND th.delta_chatelet  > 0
    AND th.heure_th_chatelet = (SELECT h FROM heure_courante)
),
score_chatelet AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_chatelet
  FROM penalites_chatelet
  GROUP BY heure_th, direction
),

-- ── AULNAY ──────────────────────────────────────────────────
penalites_aulnay AS (
  SELECT
    th.heure_th_aulnay         AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_aulnay - th.delta_aulnay)::numeric / th.delta_aulnay
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_aulnay  IS NOT NULL
    AND re.delta_aulnay  IS NOT NULL
    AND th.delta_aulnay  > 0
    AND th.heure_th_aulnay = (SELECT h FROM heure_courante)
),
score_aulnay AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_aulnay
  FROM penalites_aulnay
  GROUP BY heure_th, direction
),

-- ── CDG1 ────────────────────────────────────────────────────
penalites_cdg1 AS (
  SELECT
    th.heure_th_cdg1           AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_cdg1 - th.delta_cdg1)::numeric / th.delta_cdg1
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_cdg1  IS NOT NULL
    AND re.delta_cdg1  IS NOT NULL
    AND th.delta_cdg1  > 0
    AND th.heure_th_cdg1 = (SELECT h FROM heure_courante)
),
score_cdg1 AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_cdg1
  FROM penalites_cdg1
  GROUP BY heure_th, direction
),

-- ── VERT-GALANT ─────────────────────────────────────────────
penalites_vert_galant AS (
  SELECT
    th.heure_th_vert_galant    AS heure_th,
    th.direction,
    GREATEST(0.0,
      (re.delta_vert_galant - th.delta_vert_galant)::numeric / th.delta_vert_galant
    ) AS penalite
  FROM regularite_theorique th
  INNER JOIN regularite_reelle re ON th.mission = re.mission
  WHERE th.delta_vert_galant  IS NOT NULL
    AND re.delta_vert_galant  IS NOT NULL
    AND th.delta_vert_galant  > 0
    AND th.heure_th_vert_galant = (SELECT h FROM heure_courante)
),
score_vert_galant AS (
  SELECT
    heure_th,
    direction,
    GREATEST(0, ROUND((1 - AVG(penalite)) * 100)) AS score_regularite_vert_galant
  FROM penalites_vert_galant
  GROUP BY heure_th, direction
),

-- ── UNION des combinaisons (heure_th, direction) présentes ──
all_keys AS (
  SELECT heure_th, direction FROM score_sceaux
  UNION
  SELECT heure_th, direction FROM score_antony
  UNION
  SELECT heure_th, direction FROM score_bourg_la_reine
  UNION
  SELECT heure_th, direction FROM score_chatelet
  UNION
  SELECT heure_th, direction FROM score_aulnay
  UNION
  SELECT heure_th, direction FROM score_cdg1
  UNION
  SELECT heure_th, direction FROM score_vert_galant
)

-- ── RÉSULTAT FINAL (format large) ───────────────────────────
SELECT NOW() AT TIME ZONE 'Europe/Paris' as date_observation,
  k.heure_th,
  k.direction,
  s.score_regularite_sceaux,
  a.score_regularite_antony,
  b.score_regularite_bourg_la_reine,
  c.score_regularite_chatelet,
  au.score_regularite_aulnay,
  cd.score_regularite_cdg1,
  vg.score_regularite_vert_galant
FROM all_keys k
LEFT JOIN score_sceaux          s  USING (heure_th, direction)
LEFT JOIN score_antony          a  USING (heure_th, direction)
LEFT JOIN score_bourg_la_reine  b  USING (heure_th, direction)
LEFT JOIN score_chatelet        c  USING (heure_th, direction)
LEFT JOIN score_aulnay          au USING (heure_th, direction)
LEFT JOIN score_cdg1            cd USING (heure_th, direction)
LEFT JOIN score_vert_galant     vg USING (heure_th, direction)
ORDER BY k.heure_th, k.direction

"""


def reconstruire_table(engine: Engine) -> None:
    """Supprime et recrée la table regularite_theorique à partir des horaires théoriques.

    Calcule pour chaque heure et direction le delta moyen entre trains consécutifs
    sur 7 gares clés du RER B, en ne conservant que la première observation
    de chaque mission.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_RECONSTRUCTION))
    print(f"Table '{TABLE_CIBLE}' reconstruite.")


def reconstruire_table_reelle(engine: Engine) -> None:
    """Supprime et recrée la table regularite_reelle à partir des horaires réels.

    Calcule pour chaque heure et direction le delta moyen entre trains consécutifs
    sur 7 gares clés du RER B, en prenant le passage le plus tardif (MAX) par mission.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_RECONSTRUCTION_REELLE))
    print(f"Table '{TABLE_CIBLE_REELLE}' reconstruite.")


def creer_table_histo_regularite(engine: Engine) -> None:
    """Crée la table hist_moyenne_regularite si elle n'existe pas encore.


    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATION_HISTO_REG))


def inserer_snapshot_regularite(engine: Engine) -> None:
    """Insère un snapshot de l'écart relatif réel/théorique pour l'heure courante.

    Compare regularite_reelle et regularite_theorique et calcule pour chaque
    gare et direction le ratio (réel - théorique) / théorique, filtré sur
    l'heure d'exécution.

    Args:
        engine: Engine SQLAlchemy connecté à Postgres.
    """
    with engine.begin() as conn:
        resultat = conn.execute(text(SQL_INSERTION_HISTO_REG))
        print(f"Snapshot régularité inséré : {resultat.rowcount} ligne(s) → {TABLE_HISTO_REG}")


def main() -> None:
    """Point d'entrée du script."""
    engine = creer_engine()
    reconstruire_table(engine)
    reconstruire_table_reelle(engine)
    creer_table_histo_regularite(engine)
    inserer_snapshot_regularite(engine)


if __name__ == "__main__":
    main()
