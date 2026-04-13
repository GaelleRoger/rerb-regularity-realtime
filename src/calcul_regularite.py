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
            (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q','N') THEN 'Nord' ELSE 'Sud' END) AS direction
        FROM horaires_theoriques_trv
    ),

    delta_heure AS (
        SELECT
            date_min, mission, direction,
            EXTRACT(hour FROM sceaux::timestamp)              AS heure_sceaux,
            LAG(sceaux) OVER (PARTITION BY direction ORDER BY sceaux)                        AS sceaux_precedent, sceaux,
            EXTRACT(hour FROM antony::timestamp)              AS heure_antony,
            LAG(antony) OVER (PARTITION BY direction ORDER BY antony)                        AS antony_precedent, antony,
            EXTRACT(hour FROM bourg_la_reine::timestamp)      AS heure_bourg_la_reine,
            LAG(bourg_la_reine) OVER (PARTITION BY direction ORDER BY bourg_la_reine)        AS bourg_la_reine_precedent, bourg_la_reine,
            EXTRACT(hour FROM chatelet_les_halles::timestamp) AS heure_chatelet,
            LAG(chatelet_les_halles) OVER (PARTITION BY direction ORDER BY chatelet_les_halles) AS chatelet_precedent, chatelet_les_halles,
            EXTRACT(hour FROM aulnay_sous_bois::timestamp)    AS heure_aulnay,
            LAG(aulnay_sous_bois) OVER (PARTITION BY direction ORDER BY aulnay_sous_bois)    AS aulnay_precedent, aulnay_sous_bois,
            EXTRACT(hour FROM aeroport_cdg_1_rer::timestamp)  AS heure_cdg1,
            LAG(aeroport_cdg_1_rer) OVER (PARTITION BY direction ORDER BY aeroport_cdg_1_rer) AS cdg1_precedent, aeroport_cdg_1_rer,
            EXTRACT(hour FROM vert_galant::timestamp)         AS heure_vert_galant,
            LAG(vert_galant) OVER (PARTITION BY direction ORDER BY vert_galant)              AS vert_galant_precedent, vert_galant
        FROM init
        WHERE date_observation = date_min
        ORDER BY direction, chatelet_les_halles
    ),

    sceaux AS (
        SELECT mission, direction, heure_sceaux, sceaux, sceaux_precedent,
            ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER AS delta_sceaux
        FROM delta_heure WHERE sceaux IS NOT NULL
    ),
    antony AS (
        SELECT mission, direction, heure_antony, antony, antony_precedent,
            ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER AS delta_antony
        FROM delta_heure WHERE antony IS NOT NULL
    ),
    bourg_la_reine AS (
        SELECT mission, direction, heure_bourg_la_reine, bourg_la_reine, bourg_la_reine_precedent,
            ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine
        FROM delta_heure WHERE bourg_la_reine IS NOT NULL
    ),
    chatelet AS (
        SELECT mission, direction, heure_chatelet, chatelet_les_halles, chatelet_precedent,
            ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER AS delta_chatelet
        FROM delta_heure WHERE chatelet_les_halles IS NOT NULL
    ),
    aulnay AS (
        SELECT mission, direction, heure_aulnay, aulnay_sous_bois, aulnay_precedent,
            ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER AS delta_aulnay
        FROM delta_heure WHERE aulnay_sous_bois IS NOT NULL
    ),
    cdg1 AS (
        SELECT mission, direction, heure_cdg1, aeroport_cdg_1_rer, cdg1_precedent,
            ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER AS delta_cdg1
        FROM delta_heure WHERE aeroport_cdg_1_rer IS NOT NULL
    ),
    vert_galant AS (
        SELECT mission, direction, heure_vert_galant, vert_galant, vert_galant_precedent,
            ROUND(EXTRACT(EPOCH FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) / 60)::INTEGER AS delta_vert_galant
        FROM delta_heure WHERE vert_galant IS NOT NULL
    ),

    agg_sceaux AS (
        SELECT heure_sceaux AS heure, direction,
            AVG(delta_sceaux)  AS delta_sceaux_min,
            MAX(delta_sceaux)  AS max_delta_sceaux
        FROM sceaux GROUP BY 1, 2
    ),
    agg_antony AS (
        SELECT heure_antony AS heure, direction,
            AVG(delta_antony)  AS delta_antony_min,
            MAX(delta_antony)  AS max_delta_antony
        FROM antony GROUP BY 1, 2
    ),
    agg_bourg_la_reine AS (
        SELECT heure_bourg_la_reine AS heure, direction,
            AVG(delta_bourg_la_reine)  AS delta_bourg_la_reine_min,
            MAX(delta_bourg_la_reine)  AS max_delta_bourg_la_reine
        FROM bourg_la_reine GROUP BY 1, 2
    ),
    agg_chatelet AS (
        SELECT heure_chatelet AS heure, direction,
            AVG(delta_chatelet)  AS delta_chatelet_min,
            MAX(delta_chatelet)  AS max_delta_chatelet
        FROM chatelet GROUP BY 1, 2
    ),
    agg_aulnay AS (
        SELECT heure_aulnay AS heure, direction,
            AVG(delta_aulnay)  AS delta_aulnay_min,
            MAX(delta_aulnay)  AS max_delta_aulnay
        FROM aulnay GROUP BY 1, 2
    ),
    agg_cdg1 AS (
        SELECT heure_cdg1 AS heure, direction,
            AVG(delta_cdg1)  AS delta_cdg1_min,
            MAX(delta_cdg1)  AS max_delta_cdg1
        FROM cdg1 GROUP BY 1, 2
    ),
    agg_vert_galant AS (
        SELECT heure_vert_galant AS heure, direction,
            AVG(delta_vert_galant)  AS delta_vert_galant_min,
            MAX(delta_vert_galant)  AS max_delta_vert_galant
        FROM vert_galant GROUP BY 1, 2
    )

    SELECT
        COALESCE(s.heure, a.heure, b.heure, c.heure, au.heure, cdg.heure, vg.heure) AS heure,
        COALESCE(s.direction, a.direction, b.direction, c.direction, au.direction, cdg.direction, vg.direction) AS direction,
        s.delta_sceaux_min,         s.max_delta_sceaux,
        a.delta_antony_min,         a.max_delta_antony,
        b.delta_bourg_la_reine_min, b.max_delta_bourg_la_reine,
        c.delta_chatelet_min,       c.max_delta_chatelet,
        au.delta_aulnay_min,        au.max_delta_aulnay,
        cdg.delta_cdg1_min,         cdg.max_delta_cdg1,
        vg.delta_vert_galant_min,   vg.max_delta_vert_galant
    FROM agg_chatelet  c
        FULL OUTER JOIN agg_antony       a   ON c.heure = a.heure   AND c.direction = a.direction
        FULL OUTER JOIN agg_bourg_la_reine b ON c.heure = b.heure   AND c.direction = b.direction
        FULL OUTER JOIN agg_sceaux     s     ON c.heure = s.heure   AND c.direction = s.direction
        FULL OUTER JOIN agg_aulnay       au  ON c.heure = au.heure  AND c.direction = au.direction
        FULL OUTER JOIN agg_cdg1         cdg ON c.heure = cdg.heure AND c.direction = cdg.direction
        FULL OUTER JOIN agg_vert_galant  vg  ON c.heure = vg.heure  AND c.direction = vg.direction
    ORDER BY heure, direction
"""


TABLE_CIBLE_REELLE = "regularite_reelle"

SQL_RECONSTRUCTION_REELLE = f"""
    DROP TABLE IF EXISTS {TABLE_CIBLE_REELLE};

    CREATE TABLE {TABLE_CIBLE_REELLE} AS
    WITH init AS (
        SELECT
            mission,
            (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q') THEN 'Nord' ELSE 'Sud' END) AS direction,
            MAX(sceaux) AS sceaux, MAX(antony) AS antony,
            MAX(bourg_la_reine) AS bourg_la_reine, MAX(chatelet_les_halles) AS chatelet_les_halles,
            MAX(aulnay_sous_bois) AS aulnay_sous_bois,
            MAX(aeroport_cdg_1_rer) AS aeroport_cdg_1_rer, MAX(vert_galant) AS vert_galant
        FROM horaires_reels_trv
        GROUP BY 1, 2
        ORDER BY direction, chatelet_les_halles
    ),

    delta_heure AS (
        SELECT
            mission, direction,
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
        ORDER BY direction, chatelet_les_halles
    ),

    sceaux AS (
        SELECT mission, direction, heure_sceaux, sceaux, sceaux_precedent,
            ROUND(EXTRACT(EPOCH FROM (sceaux::timestamp - sceaux_precedent::timestamp)) / 60)::INTEGER AS delta_sceaux
        FROM delta_heure WHERE sceaux IS NOT NULL
    ),
    antony AS (
        SELECT mission, direction, heure_antony, antony, antony_precedent,
            ROUND(EXTRACT(EPOCH FROM (antony::timestamp - antony_precedent::timestamp)) / 60)::INTEGER AS delta_antony
        FROM delta_heure WHERE antony IS NOT NULL
    ),
    bourg_la_reine AS (
        SELECT mission, direction, heure_bourg_la_reine, bourg_la_reine, bourg_la_reine_precedent,
            ROUND(EXTRACT(EPOCH FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) / 60)::INTEGER AS delta_bourg_la_reine
        FROM delta_heure WHERE bourg_la_reine IS NOT NULL
    ),
    chatelet AS (
        SELECT mission, direction, heure_chatelet, chatelet_les_halles, chatelet_precedent,
            ROUND(EXTRACT(EPOCH FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) / 60)::INTEGER AS delta_chatelet
        FROM delta_heure WHERE chatelet_les_halles IS NOT NULL
    ),
    aulnay AS (
        SELECT mission, direction, heure_aulnay, aulnay_sous_bois, aulnay_precedent,
            ROUND(EXTRACT(EPOCH FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) / 60)::INTEGER AS delta_aulnay
        FROM delta_heure WHERE aulnay_sous_bois IS NOT NULL
    ),
    cdg1 AS (
        SELECT mission, direction, heure_cdg1, aeroport_cdg_1_rer, cdg1_precedent,
            ROUND(EXTRACT(EPOCH FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) / 60)::INTEGER AS delta_cdg1
        FROM delta_heure WHERE aeroport_cdg_1_rer IS NOT NULL
    ),
    vert_galant AS (
        SELECT mission, direction, heure_vert_galant, vert_galant, vert_galant_precedent,
            ROUND(EXTRACT(EPOCH FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) / 60)::INTEGER AS delta_vert_galant
        FROM delta_heure WHERE vert_galant IS NOT NULL
    ),

    agg_sceaux AS (
        SELECT heure_sceaux AS heure, direction,
            AVG(delta_sceaux)  AS delta_sceaux_min,
            MAX(delta_sceaux)  AS max_delta_sceaux
        FROM sceaux GROUP BY 1, 2
    ),
    agg_antony AS (
        SELECT heure_antony AS heure, direction,
            AVG(delta_antony)  AS delta_antony_min,
            MAX(delta_antony)  AS max_delta_antony
        FROM antony GROUP BY 1, 2
    ),
    agg_bourg_la_reine AS (
        SELECT heure_bourg_la_reine AS heure, direction,
            AVG(delta_bourg_la_reine)  AS delta_bourg_la_reine_min,
            MAX(delta_bourg_la_reine)  AS max_delta_bourg_la_reine
        FROM bourg_la_reine GROUP BY 1, 2
    ),
    agg_chatelet AS (
        SELECT heure_chatelet AS heure, direction,
            AVG(delta_chatelet)  AS delta_chatelet_min,
            MAX(delta_chatelet)  AS max_delta_chatelet
        FROM chatelet GROUP BY 1, 2
    ),
    agg_aulnay AS (
        SELECT heure_aulnay AS heure, direction,
            AVG(delta_aulnay)  AS delta_aulnay_min,
            MAX(delta_aulnay)  AS max_delta_aulnay
        FROM aulnay GROUP BY 1, 2
    ),
    agg_cdg1 AS (
        SELECT heure_cdg1 AS heure, direction,
            AVG(delta_cdg1)  AS delta_cdg1_min,
            MAX(delta_cdg1)  AS max_delta_cdg1
        FROM cdg1 GROUP BY 1, 2
    ),
    agg_vert_galant AS (
        SELECT heure_vert_galant AS heure, direction,
            AVG(delta_vert_galant)  AS delta_vert_galant_min,
            MAX(delta_vert_galant)  AS max_delta_vert_galant
        FROM vert_galant GROUP BY 1, 2
    )

    SELECT
        COALESCE(s.heure, a.heure, b.heure, c.heure, au.heure, cdg.heure, vg.heure) AS heure,
        COALESCE(s.direction, a.direction, b.direction, c.direction, au.direction, cdg.direction, vg.direction) AS direction,
        s.delta_sceaux_min,         s.max_delta_sceaux,
        a.delta_antony_min,         a.max_delta_antony,
        b.delta_bourg_la_reine_min, b.max_delta_bourg_la_reine,
        c.delta_chatelet_min,       c.max_delta_chatelet,
        au.delta_aulnay_min,        au.max_delta_aulnay,
        cdg.delta_cdg1_min,         cdg.max_delta_cdg1,
        vg.delta_vert_galant_min,   vg.max_delta_vert_galant
    FROM agg_chatelet  c
        FULL OUTER JOIN agg_antony       a   ON c.heure = a.heure   AND c.direction = a.direction
        FULL OUTER JOIN agg_bourg_la_reine b ON c.heure = b.heure   AND c.direction = b.direction
        FULL OUTER JOIN agg_sceaux     s     ON c.heure = s.heure   AND c.direction = s.direction
        FULL OUTER JOIN agg_aulnay       au  ON c.heure = au.heure  AND c.direction = au.direction
        FULL OUTER JOIN agg_cdg1         cdg ON c.heure = cdg.heure AND c.direction = cdg.direction
        FULL OUTER JOIN agg_vert_galant  vg  ON c.heure = vg.heure  AND c.direction = vg.direction
    ORDER BY heure, direction
"""


TABLE_HISTO_REG = "hist_moyenne_regularite"

SQL_CREATION_HISTO_REG = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_HISTO_REG} (
        id                           SERIAL PRIMARY KEY,
        date_observation             TIMESTAMP,
        heure                        NUMERIC,
        direction                    TEXT,
        delta_sceaux_min_pct         FLOAT,
        max_delta_sceaux_pct         FLOAT,
        delta_antony_min_pct         FLOAT,
        max_delta_antony_pct         FLOAT,
        delta_bourg_la_reine_min_pct FLOAT,
        max_delta_bourg_la_reine_pct FLOAT,
        delta_chatelet_min_pct       FLOAT,
        max_delta_chatelet_pct       FLOAT,
        delta_aulnay_min_pct         FLOAT,
        max_delta_aulnay_pct         FLOAT,
        delta_cdg1_min_pct           FLOAT,
        max_delta_cdg1_pct           FLOAT,
        delta_vert_galant_min_pct    FLOAT,
        max_delta_vert_galant_pct    FLOAT
    )
"""

SQL_INSERTION_HISTO_REG = f"""
    INSERT INTO {TABLE_HISTO_REG} (
        date_observation,
        heure, direction,
        delta_sceaux_min_pct,         max_delta_sceaux_pct,
        delta_antony_min_pct,         max_delta_antony_pct,
        delta_bourg_la_reine_min_pct, max_delta_bourg_la_reine_pct,
        delta_chatelet_min_pct,       max_delta_chatelet_pct,
        delta_aulnay_min_pct,         max_delta_aulnay_pct,
        delta_cdg1_min_pct,           max_delta_cdg1_pct,
        delta_vert_galant_min_pct,    max_delta_vert_galant_pct
    )
    WITH joined AS (
        SELECT
            th.heure, th.direction,
            th.delta_sceaux_min          AS th_delta_sceaux_min,
            re.delta_sceaux_min          AS re_delta_sceaux_min,
            th.max_delta_sceaux          AS th_max_delta_sceaux,
            re.max_delta_sceaux          AS re_max_delta_sceaux,
            th.delta_antony_min          AS th_delta_antony_min,
            re.delta_antony_min          AS re_delta_antony_min,
            th.max_delta_antony          AS th_max_delta_antony,
            re.max_delta_antony          AS re_max_delta_antony,
            th.delta_bourg_la_reine_min  AS th_delta_bourg_la_reine_min,
            re.delta_bourg_la_reine_min  AS re_delta_bourg_la_reine_min,
            th.max_delta_bourg_la_reine  AS th_max_delta_bourg_la_reine,
            re.max_delta_bourg_la_reine  AS re_max_delta_bourg_la_reine,
            th.delta_chatelet_min        AS th_delta_chatelet_min,
            re.delta_chatelet_min        AS re_delta_chatelet_min,
            th.max_delta_chatelet        AS th_max_delta_chatelet,
            re.max_delta_chatelet        AS re_max_delta_chatelet,
            th.delta_aulnay_min          AS th_delta_aulnay_min,
            re.delta_aulnay_min          AS re_delta_aulnay_min,
            th.max_delta_aulnay          AS th_max_delta_aulnay,
            re.max_delta_aulnay          AS re_max_delta_aulnay,
            th.delta_cdg1_min            AS th_delta_cdg1_min,
            re.delta_cdg1_min            AS re_delta_cdg1_min,
            th.max_delta_cdg1            AS th_max_delta_cdg1,
            re.max_delta_cdg1            AS re_max_delta_cdg1,
            th.delta_vert_galant_min     AS th_delta_vert_galant_min,
            re.delta_vert_galant_min     AS re_delta_vert_galant_min,
            th.max_delta_vert_galant     AS th_max_delta_vert_galant,
            re.max_delta_vert_galant     AS re_max_delta_vert_galant
        FROM regularite_theorique th
        LEFT JOIN regularite_reelle re
            ON  th.heure     = re.heure
            AND th.direction = re.direction
    ),
    calculated AS (
        SELECT
            heure, direction,
            (re_delta_sceaux_min         - th_delta_sceaux_min)         / NULLIF(th_delta_sceaux_min,         0) AS delta_sceaux_min_pct,
            (re_max_delta_sceaux         - th_max_delta_sceaux)         / NULLIF(th_max_delta_sceaux,         0) AS max_delta_sceaux_pct,
            (re_delta_antony_min         - th_delta_antony_min)         / NULLIF(th_delta_antony_min,         0) AS delta_antony_min_pct,
            (re_max_delta_antony         - th_max_delta_antony)         / NULLIF(th_max_delta_antony,         0) AS max_delta_antony_pct,
            (re_delta_bourg_la_reine_min - th_delta_bourg_la_reine_min) / NULLIF(th_delta_bourg_la_reine_min, 0) AS delta_bourg_la_reine_min_pct,
            (re_max_delta_bourg_la_reine - th_max_delta_bourg_la_reine) / NULLIF(th_max_delta_bourg_la_reine, 0) AS max_delta_bourg_la_reine_pct,
            (re_delta_chatelet_min       - th_delta_chatelet_min)       / NULLIF(th_delta_chatelet_min,       0) AS delta_chatelet_min_pct,
            (re_max_delta_chatelet       - th_max_delta_chatelet)       / NULLIF(th_max_delta_chatelet,       0) AS max_delta_chatelet_pct,
            (re_delta_aulnay_min         - th_delta_aulnay_min)         / NULLIF(th_delta_aulnay_min,         0) AS delta_aulnay_min_pct,
            (re_max_delta_aulnay         - th_max_delta_aulnay)         / NULLIF(th_max_delta_aulnay,         0) AS max_delta_aulnay_pct,
            (re_delta_cdg1_min           - th_delta_cdg1_min)           / NULLIF(th_delta_cdg1_min,           0) AS delta_cdg1_min_pct,
            (re_max_delta_cdg1           - th_max_delta_cdg1)           / NULLIF(th_max_delta_cdg1,           0) AS max_delta_cdg1_pct,
            (re_delta_vert_galant_min    - th_delta_vert_galant_min)    / NULLIF(th_delta_vert_galant_min,    0) AS delta_vert_galant_min_pct,
            (re_max_delta_vert_galant    - th_max_delta_vert_galant)    / NULLIF(th_max_delta_vert_galant,    0) AS max_delta_vert_galant_pct
        FROM joined
    )
    SELECT
        NOW() AT TIME ZONE 'Europe/Paris' AS date_observation,
        heure, direction,
        delta_sceaux_min_pct,         max_delta_sceaux_pct,
        delta_antony_min_pct,         max_delta_antony_pct,
        delta_bourg_la_reine_min_pct, max_delta_bourg_la_reine_pct,
        delta_chatelet_min_pct,       max_delta_chatelet_pct,
        delta_aulnay_min_pct,         max_delta_aulnay_pct,
        delta_cdg1_min_pct,           max_delta_cdg1_pct,
        delta_vert_galant_min_pct,    max_delta_vert_galant_pct
    FROM calculated c
    WHERE heure = EXTRACT(HOUR FROM NOW() AT TIME ZONE 'Europe/Paris')
    ORDER BY heure, direction
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
