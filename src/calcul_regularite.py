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
        LAG(sceaux) OVER (PARTITION BY direction ORDER BY sceaux)                          AS sceaux_precedent, sceaux,
        LAG(antony) OVER (PARTITION BY direction ORDER BY antony)                          AS antony_precedent, antony,
        LAG(bourg_la_reine) OVER (PARTITION BY direction ORDER BY bourg_la_reine)          AS bourg_la_reine_precedent, bourg_la_reine,
        LAG(chatelet_les_halles) OVER (PARTITION BY direction ORDER BY chatelet_les_halles) AS chatelet_precedent, chatelet_les_halles,
        LAG(aulnay_sous_bois) OVER (PARTITION BY direction ORDER BY aulnay_sous_bois)      AS aulnay_precedent, aulnay_sous_bois,
        LAG(aeroport_cdg_1_rer) OVER (PARTITION BY direction ORDER BY aeroport_cdg_1_rer)  AS cdg1_precedent, aeroport_cdg_1_rer,
        LAG(vert_galant) OVER (PARTITION BY direction ORDER BY vert_galant)                AS vert_galant_precedent, vert_galant
    FROM init
    WHERE date_observation = date_min
    ORDER BY direction, chatelet_les_halles
)
SELECT
    mission,
    direction,
    sceaux             AS heure_th_sceaux,
    ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER             AS delta_sceaux,
    antony             AS heure_th_antony,
    ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER             AS delta_antony,
    bourg_la_reine     AS heure_th_bourg_la_reine,
    ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine,
    chatelet_les_halles           AS heure_th_chatelet,
    ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER  AS delta_chatelet,
    aulnay_sous_bois             AS heure_th_aulnay,
    ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER   AS delta_aulnay,
    aeroport_cdg_1_rer               AS heure_th_cdg1,
    ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER   AS delta_cdg1,
    vert_galant        AS heure_th_vert_galant,
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
        LAG(sceaux) OVER (PARTITION BY direction ORDER BY sceaux)                       AS sceaux_precedent, sceaux,
        LAG(antony) OVER (PARTITION BY direction ORDER BY antony)                       AS antony_precedent, antony,
        LAG(bourg_la_reine) OVER (PARTITION BY direction ORDER BY bourg_la_reine)       AS bourg_la_reine_precedent, bourg_la_reine,
        LAG(chatelet_les_halles) OVER (PARTITION BY direction ORDER BY chatelet_les_halles) AS chatelet_precedent, chatelet_les_halles,
        LAG(aulnay_sous_bois) OVER (PARTITION BY direction ORDER BY aulnay_sous_bois)   AS aulnay_precedent, aulnay_sous_bois,
        LAG(aeroport_cdg_1_rer) OVER (PARTITION BY direction ORDER BY aeroport_cdg_1_rer) AS cdg1_precedent, aeroport_cdg_1_rer,
        LAG(vert_galant) OVER (PARTITION BY direction ORDER BY vert_galant)             AS vert_galant_precedent, vert_galant
    FROM init
    ORDER BY direction, chatelet_les_halles
)
SELECT
    mission,
    direction,
    sceaux             AS heure_re_sceaux,
    ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER             AS delta_sceaux,
    antony             AS heure_re_antony,
    ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER             AS delta_antony,
    bourg_la_reine     AS heure_re_bourg_la_reine,
    ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine,
    chatelet_les_halles           AS heure_re_chatelet,
    ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER  AS delta_chatelet,
    aulnay_sous_bois             AS heure_re_aulnay,
    ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER   AS delta_aulnay,
    aeroport_cdg_1_rer               AS heure_re_cdg1,
    ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER   AS delta_cdg1,
    vert_galant        AS heure_re_vert_galant,
    ROUND(EXTRACT(EPOCH FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) / 60)::INTEGER   AS delta_vert_galant
FROM delta_heure;
"""


TABLE_HISTO_REG = "hist_moyenne_regularite"

SQL_CREATION_HISTO_REG = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_HISTO_REG} (
        date_observation      TIMESTAMP,
        direction             TEXT,
        score_sceaux          INTEGER,
        score_antony          INTEGER,
        score_bourg_la_reine  INTEGER,
        score_chatelet        INTEGER,
        score_aulnay          INTEGER,
        score_cdg1            INTEGER,
        score_vert_galant     INTEGER
    )
"""


SQL_INSERTION_HISTO_REG = f"""
    INSERT INTO {TABLE_HISTO_REG}
-- ============================================================
-- RÉGULARITÉ PAR ARRÊT ET DIRECTION — heure à venir (NOW → NOW+60min)
-- Structure finale : arrêt | direction | score_regularite
-- ============================================================

WITH

-- ======================== SCEAUX ========================
th_sceaux AS (
    SELECT mission, delta_sceaux AS delta_th
    FROM regularite_theorique
    WHERE delta_sceaux IS NOT NULL
),
re_sceaux AS (
    SELECT mission, direction, heure_re_sceaux, delta_sceaux AS delta_re
    FROM regularite_reelle
    WHERE delta_sceaux IS NOT NULL
      AND heure_re_sceaux::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                          AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_sceaux AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_sceaux r
    INNER JOIN th_sceaux t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_sceaux AS (
    SELECT
        'sceaux'                                               AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_sceaux
    GROUP BY direction
),

-- ======================== ANTONY ========================
th_antony AS (
    SELECT mission, delta_antony AS delta_th
    FROM regularite_theorique
    WHERE delta_antony IS NOT NULL
),
re_antony AS (
    SELECT mission, direction, heure_re_antony, delta_antony AS delta_re
    FROM regularite_reelle
    WHERE delta_antony IS NOT NULL
      AND heure_re_antony::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                          AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_antony AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_antony r
    INNER JOIN th_antony t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_antony AS (
    SELECT
        'antony'                                               AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_antony
    GROUP BY direction
),

-- =================== BOURG_LA_REINE ===================
th_bourg_la_reine AS (
    SELECT mission, delta_bourg_la_reine AS delta_th
    FROM regularite_theorique
    WHERE delta_bourg_la_reine IS NOT NULL
),
re_bourg_la_reine AS (
    SELECT mission, direction, heure_re_bourg_la_reine, delta_bourg_la_reine AS delta_re
    FROM regularite_reelle
    WHERE delta_bourg_la_reine IS NOT NULL
      AND heure_re_bourg_la_reine::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                                  AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_bourg_la_reine AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_bourg_la_reine r
    INNER JOIN th_bourg_la_reine t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_bourg_la_reine AS (
    SELECT
        'bourg_la_reine'                                       AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_bourg_la_reine
    GROUP BY direction
),

-- ======================== CHATELET ========================
th_chatelet AS (
    SELECT mission, delta_chatelet AS delta_th
    FROM regularite_theorique
    WHERE delta_chatelet IS NOT NULL
),
re_chatelet AS (
    SELECT mission, direction, heure_re_chatelet, delta_chatelet AS delta_re
    FROM regularite_reelle
    WHERE delta_chatelet IS NOT NULL
      AND heure_re_chatelet::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                           AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_chatelet AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_chatelet r
    INNER JOIN th_chatelet t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_chatelet AS (
    SELECT
        'chatelet'                                             AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_chatelet
    GROUP BY direction
),

-- ======================== AULNAY ========================
th_aulnay AS (
    SELECT mission, delta_aulnay AS delta_th
    FROM regularite_theorique
    WHERE delta_aulnay IS NOT NULL
),
re_aulnay AS (
    SELECT mission, direction, heure_re_aulnay, delta_aulnay AS delta_re
    FROM regularite_reelle
    WHERE delta_aulnay IS NOT NULL
      AND heure_re_aulnay::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                          AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_aulnay AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_aulnay r
    INNER JOIN th_aulnay t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_aulnay AS (
    SELECT
        'aulnay'                                               AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_aulnay
    GROUP BY direction
),

-- ======================== CDG1 ========================
th_cdg1 AS (
    SELECT mission, delta_cdg1 AS delta_th
    FROM regularite_theorique
    WHERE delta_cdg1 IS NOT NULL
),
re_cdg1 AS (
    SELECT mission, direction, heure_re_cdg1, delta_cdg1 AS delta_re
    FROM regularite_reelle
    WHERE delta_cdg1 IS NOT NULL
      AND heure_re_cdg1::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                        AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_cdg1 AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_cdg1 r
    INNER JOIN th_cdg1 t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_cdg1 AS (
    SELECT
        'cdg1'                                                 AS arret,
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_cdg1
    GROUP BY direction
),

-- ====================== VERT_GALANT ======================
th_vert_galant AS (
    SELECT mission, delta_vert_galant AS delta_th
    FROM regularite_theorique
    WHERE delta_vert_galant IS NOT NULL
),
re_vert_galant AS (
    SELECT mission, direction, heure_re_vert_galant, delta_vert_galant AS delta_re
    FROM regularite_reelle
    WHERE delta_vert_galant IS NOT NULL
      AND heure_re_vert_galant::timestamp BETWEEN (NOW() AT TIME ZONE 'Europe/Paris')
                                              AND (NOW() AT TIME ZONE 'Europe/Paris' + INTERVAL '60 minutes')
),
join_vert_galant AS (
    SELECT r.direction, r.delta_re, t.delta_th
    FROM re_vert_galant r
    INNER JOIN th_vert_galant t ON t.mission = r.mission
    WHERE r.delta_re IS NOT NULL AND t.delta_th IS NOT NULL AND t.delta_th > 0
),
score_vert_galant AS (
    SELECT
        direction,
        GREATEST(0, ROUND((1 - AVG(GREATEST(0.0, (delta_re - delta_th)::numeric / delta_th))) * 100)) AS score_regularite
    FROM join_vert_galant
    GROUP BY direction
)

-- ==================== TABLE FINALE (PIVOT) ====================
SELECT
    NOW() AT TIME ZONE 'Europe/Paris'      AS date_observation,
    COALESCE(
        s.direction, a.direction, blr.direction,
        c.direction, au.direction, cd.direction, vg.direction
    )                                      AS direction,
    s.score_regularite                     AS score_sceaux,
    a.score_regularite                     AS score_antony,
    blr.score_regularite                   AS score_bourg_la_reine,
    c.score_regularite                     AS score_chatelet,
    au.score_regularite                    AS score_aulnay,
    cd.score_regularite                    AS score_cdg1,
    vg.score_regularite                    AS score_vert_galant
FROM      score_sceaux         s
FULL JOIN score_antony          a   USING (direction)
FULL JOIN score_bourg_la_reine blr USING (direction)
FULL JOIN score_chatelet        c   USING (direction)
FULL JOIN score_aulnay          au  USING (direction)
FULL JOIN score_cdg1            cd  USING (direction)
FULL JOIN score_vert_galant     vg  USING (direction)
ORDER BY direction

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