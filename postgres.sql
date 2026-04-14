-- API
SELECT * FROM hist_moyenne_regularite
ORDER BY date_observation DESC
LIMIT 2;

SELECT * FROM hist_moyenne_allongement
ORDER BY date_calcul DESC
LIMIT 2;






WITH init as (select type_mission, gare_depart, nb_arrets_desservis,
MAX(nb_arrets_desservis) OVER (PARTITION BY type_mission) as nb_arrets_max
from horaires_theoriques)

SELECT distinct type_mission, gare_depart
from init
WHERE nb_arrets_desservis = nb_arrets_max


WITH init as (select type_mission, gare_depart, nb_arrets_desservis, date_observation,
MAX(nb_arrets_desservis) OVER (PARTITION BY type_mission) as nb_arrets_max,
MAX(date_observation) OVER () as date_max
from horaires_theoriques)

SELECT distinct type_mission, gare_depart
from init
WHERE nb_arrets_desservis = nb_arrets_max
AND date_observation = date_max

WITH init as (select * from referentiel_missions),
tmp as (SELECT type_mission as mission_tmp, gare_depart as depart_tmp
FROM referentiel_missions_tmp)
SELECT COALESCE(type_mission, mission_tmp) as code_mission,
COALESCE(gare_depart,depart_tmp) as gare_depart
FROM init
FULL OUTER JOIN tmp
ON init.type_mission = tmp.mission_tmp


WITH premiere_derniere AS (
    SELECT
        mission,
        MIN(date_observation) AS premiere_observation,
        MAX(date_observation) AS derniere_observation
    FROM hist_ecart_horaire
    GROUP BY 1
),
ecarts AS (
    SELECT
        pd.mission,
        pd.premiere_observation,
        pd.derniere_observation,
        MAX(pd.derniere_observation) OVER () as date_max, -- on calcule la dernière date d'observation dans toute la table
        premier.ecart_max   AS ecart_premier,
        dernier.ecart_max   AS ecart_dernier
    FROM premiere_derniere pd
    -- Retard à la première apparition
    JOIN hist_ecart_horaire premier
        ON premier.mission          = pd.mission
        AND premier.date_observation = pd.premiere_observation
    -- Retard à la dernière apparition
    JOIN hist_ecart_horaire dernier
        ON dernier.mission          = pd.mission
        AND dernier.date_observation = pd.derniere_observation
),
ratio as (
SELECT
    mission,
    premiere_observation,
    derniere_observation,
    ecart_premier,
    ecart_dernier,
    (EXTRACT(HOUR FROM (derniere_observation - premiere_observation))*60 + 
    EXTRACT(MINUTE FROM (derniere_observation - premiere_observation))) as temps_circule_min,
    COALESCE(ecart_dernier - ecart_premier, 0) AS evolution_ecart
FROM ecarts
WHERE derniere_observation = date_max -- on ne regarde que les trains qui circulent
),
allongement as (
SELECT mission, premiere_observation, derniere_observation, ecart_premier, ecart_dernier, 
temps_circule_min, evolution_ecart, evolution_ecart/(temps_circule_min+1) as allongement_parcours
FROM ratio)
SELECT NOW() AT TIME ZONE 'Europe/Paris' as date_calcul, AVG(allongement_parcours) as moyenne_allongement
FROM allongement;


-- Régularité théorique
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

-- Régularité réelle

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



-- score régularité

with th_chatelet as (select mission, direction, heure_th_chatelet, delta_chatelet as delta_th_chatelet
from regularite_theorique
where delta_chatelet is not null
),
re_chatelet as (
select mission, delta_chatelet as delta_re_chatelet
from regularite_reelle
where delta_chatelet is not null
),
jointure as (
select th_chatelet.mission, direction, heure_th_chatelet, delta_th_chatelet, delta_re_chatelet
from th_chatelet
inner join re_chatelet
on th_chatelet.mission = re_chatelet.mission
),
penalites AS (
  SELECT
    heure_th_chatelet,
    direction,
    GREATEST(0.0,
      (delta_re_chatelet - delta_th_chatelet)::numeric / delta_th_chatelet
    ) AS penalite
  FROM jointure
  WHERE
    delta_re_chatelet IS NOT NULL
    AND delta_th_chatelet IS NOT NULL
    AND delta_th_chatelet > 0
)

SELECT
  heure_th_chatelet as heure_th,
  direction,
  GREATEST(0, ROUND((1 - AVG(penalite)) * 100))    AS score_regularite_chatelet
FROM penalites
WHERE heure_th_chatelet = EXTRACT (HOUR FROM NOW() AT TIME ZONE 'Europe/Paris')
GROUP BY heure_th_chatelet, direction
ORDER BY heure_th_chatelet, direction;






/************** OLD REGUL ************/

WITH init AS (
    SELECT 
        date_observation,
        MIN(date_observation) OVER (PARTITION BY mission) AS date_min,
        mission, sceaux, antony, bourg_la_reine, chatelet_les_halles,
        aulnay_sous_bois, aeroport_cdg_1_rer, vert_galant,
        (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q') THEN 'Nord' ELSE 'Sud' END) AS direction
    FROM horaires_theoriques_trv
),

delta_heure AS (
    SELECT 
        date_min, mission, direction,
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
    WHERE date_observation = date_min
    ORDER BY direction, chatelet_les_halles
),

-- CTEs de calcul du delta par gare
sceaux AS (
    SELECT mission, direction, heure_sceaux, sceaux, sceaux_precedent,
        EXTRACT(minute FROM (sceaux::timestamp - sceaux_precedent::timestamp)) AS delta_sceaux
    FROM delta_heure WHERE sceaux IS NOT NULL
),
antony AS (
    SELECT mission, direction, heure_antony, antony, antony_precedent,
        EXTRACT(minute FROM (antony::timestamp - antony_precedent::timestamp)) AS delta_antony
    FROM delta_heure WHERE antony IS NOT NULL
),
bourg_la_reine AS (
    SELECT mission, direction, heure_bourg_la_reine, bourg_la_reine, bourg_la_reine_precedent,
        EXTRACT(minute FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) AS delta_bourg_la_reine
    FROM delta_heure WHERE bourg_la_reine IS NOT NULL
),
chatelet AS (
    SELECT mission, direction, heure_chatelet, chatelet_les_halles, chatelet_precedent,
        EXTRACT(minute FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) AS delta_chatelet
    FROM delta_heure WHERE chatelet_les_halles IS NOT NULL
),
aulnay AS (
    SELECT mission, direction, heure_aulnay, aulnay_sous_bois, aulnay_precedent,
        EXTRACT(minute FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) AS delta_aulnay
    FROM delta_heure WHERE aulnay_sous_bois IS NOT NULL
),
cdg1 AS (
    SELECT mission, direction, heure_cdg1, aeroport_cdg_1_rer, cdg1_precedent,
        EXTRACT(minute FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) AS delta_cdg1
    FROM delta_heure WHERE aeroport_cdg_1_rer IS NOT NULL
),
vert_galant AS (
    SELECT mission, direction, heure_vert_galant, vert_galant, vert_galant_precedent,
        EXTRACT(minute FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) AS delta_vert_galant
    FROM delta_heure WHERE vert_galant IS NOT NULL
),

-- CTEs d'agrégation par heure et direction
agg_sceaux AS (
    SELECT heure_sceaux AS heure, direction,
        AVG(delta_sceaux)          AS delta_sceaux_min,
        COUNT(DISTINCT mission)    AS nb_missions_sceaux
    FROM sceaux GROUP BY 1, 2
),
agg_antony AS (
    SELECT heure_antony AS heure, direction,
        AVG(delta_antony)          AS delta_antony_min,
        COUNT(DISTINCT mission)    AS nb_missions_antony
    FROM antony GROUP BY 1, 2
),
agg_bourg_la_reine AS (
    SELECT heure_bourg_la_reine AS heure, direction,
        AVG(delta_bourg_la_reine)  AS delta_bourg_la_reine_min,
        COUNT(DISTINCT mission)    AS nb_missions_bourg_la_reine
    FROM bourg_la_reine GROUP BY 1, 2
),
agg_chatelet AS (
    SELECT heure_chatelet AS heure, direction,
        AVG(delta_chatelet)        AS delta_chatelet_min,
        COUNT(DISTINCT mission)    AS nb_missions_chatelet
    FROM chatelet GROUP BY 1, 2
),
agg_aulnay AS (
    SELECT heure_aulnay AS heure, direction,
        AVG(delta_aulnay)          AS delta_aulnay_min,
        COUNT(DISTINCT mission)    AS nb_missions_aulnay
    FROM aulnay GROUP BY 1, 2
),
agg_cdg1 AS (
    SELECT heure_cdg1 AS heure, direction,
        AVG(delta_cdg1)            AS delta_cdg1_min,
        COUNT(DISTINCT mission)    AS nb_missions_cdg1
    FROM cdg1 GROUP BY 1, 2
),
agg_vert_galant AS (
    SELECT heure_vert_galant AS heure, direction,
        AVG(delta_vert_galant)     AS delta_vert_galant_min,
        COUNT(DISTINCT mission)    AS nb_missions_vert_galant
    FROM vert_galant GROUP BY 1, 2
)

-- Assemblage final
SELECT
    COALESCE(s.heure, a.heure, b.heure, c.heure, au.heure, cdg.heure, vg.heure) AS heure,
    COALESCE(s.direction, a.direction, b.direction, c.direction, au.direction, cdg.direction, vg.direction) AS direction,
    s.delta_sceaux_min,          s.nb_missions_sceaux,
    a.delta_antony_min,          a.nb_missions_antony,
    b.delta_bourg_la_reine_min,  b.nb_missions_bourg_la_reine,
    c.delta_chatelet_min,        c.nb_missions_chatelet,
    au.delta_aulnay_min,         au.nb_missions_aulnay,
    cdg.delta_cdg1_min,          cdg.nb_missions_cdg1,
    vg.delta_vert_galant_min,    vg.nb_missions_vert_galant
FROM agg_sceaux s
    FULL OUTER JOIN agg_antony       a   ON s.heure = a.heure   AND s.direction = a.direction
    FULL OUTER JOIN agg_bourg_la_reine b ON s.heure = b.heure   AND s.direction = b.direction
    FULL OUTER JOIN agg_chatelet     c   ON s.heure = c.heure   AND s.direction = c.direction
    FULL OUTER JOIN agg_aulnay       au  ON s.heure = au.heure  AND s.direction = au.direction
    FULL OUTER JOIN agg_cdg1         cdg ON s.heure = cdg.heure AND s.direction = cdg.direction
    FULL OUTER JOIN agg_vert_galant  vg  ON s.heure = vg.heure  AND s.direction = vg.direction
ORDER BY heure, direction
;



-- on prend l'horaire max affiché par arrêt pour faire le calcul en du passage réel
WITH init as (SELECT 
        mission, (CASE WHEN SUBSTRING(mission,1,1) IN ('E','I','J','O','Q') THEN 'Nord' ELSE 'Sud' END) AS direction,
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
),

-- CTEs de calcul du delta par gare
sceaux AS (
    SELECT mission, direction, heure_sceaux, sceaux, sceaux_precedent,
        EXTRACT(minute FROM (sceaux::timestamp - sceaux_precedent::timestamp)) AS delta_sceaux
    FROM delta_heure WHERE sceaux IS NOT NULL
),
antony AS (
    SELECT mission, direction, heure_antony, antony, antony_precedent,
        EXTRACT(minute FROM (antony::timestamp - antony_precedent::timestamp)) AS delta_antony
    FROM delta_heure WHERE antony IS NOT NULL
),
bourg_la_reine AS (
    SELECT mission, direction, heure_bourg_la_reine, bourg_la_reine, bourg_la_reine_precedent,
        EXTRACT(minute FROM (bourg_la_reine::timestamp - bourg_la_reine_precedent::timestamp)) AS delta_bourg_la_reine
    FROM delta_heure WHERE bourg_la_reine IS NOT NULL
),
chatelet AS (
    SELECT mission, direction, heure_chatelet, chatelet_les_halles, chatelet_precedent,
        EXTRACT(minute FROM (chatelet_les_halles::timestamp - chatelet_precedent::timestamp)) AS delta_chatelet
    FROM delta_heure WHERE chatelet_les_halles IS NOT NULL
),
aulnay AS (
    SELECT mission, direction, heure_aulnay, aulnay_sous_bois, aulnay_precedent,
        EXTRACT(minute FROM (aulnay_sous_bois::timestamp - aulnay_precedent::timestamp)) AS delta_aulnay
    FROM delta_heure WHERE aulnay_sous_bois IS NOT NULL
),
cdg1 AS (
    SELECT mission, direction, heure_cdg1, aeroport_cdg_1_rer, cdg1_precedent,
        EXTRACT(minute FROM (aeroport_cdg_1_rer::timestamp - cdg1_precedent::timestamp)) AS delta_cdg1
    FROM delta_heure WHERE aeroport_cdg_1_rer IS NOT NULL
),
vert_galant AS (
    SELECT mission, direction, heure_vert_galant, vert_galant, vert_galant_precedent,
        EXTRACT(minute FROM (vert_galant::timestamp - vert_galant_precedent::timestamp)) AS delta_vert_galant
    FROM delta_heure WHERE vert_galant IS NOT NULL
),

-- CTEs d'agrégation par heure et direction
agg_sceaux AS (
    SELECT heure_sceaux AS heure, direction,
        AVG(delta_sceaux)          AS delta_sceaux_min,
        MAX(delta_sceaux)          AS max_delta_sceaux
    FROM sceaux GROUP BY 1, 2
),
agg_antony AS (
    SELECT heure_antony AS heure, direction,
        AVG(delta_antony)          AS delta_antony_min,
        MAX(delta_antony)          AS max_delta_antony
    FROM antony GROUP BY 1, 2
),
agg_bourg_la_reine AS (
    SELECT heure_bourg_la_reine AS heure, direction,
        AVG(delta_bourg_la_reine)  AS delta_bourg_la_reine_min,
        MAX(delta_bourg_la_reine)          AS max_delta_bourg_la_reine
    FROM bourg_la_reine GROUP BY 1, 2
),
agg_chatelet AS (
    SELECT heure_chatelet AS heure, direction,
        AVG(delta_chatelet)        AS delta_chatelet_min,
        MAX(delta_chatelet)          AS max_delta_chatelet
    FROM chatelet GROUP BY 1, 2
),
agg_aulnay AS (
    SELECT heure_aulnay AS heure, direction,
        AVG(delta_aulnay)          AS delta_aulnay_min,
        MAX(delta_aulnay)          AS max_delta_aulnay
    FROM aulnay GROUP BY 1, 2
),
agg_cdg1 AS (
    SELECT heure_cdg1 AS heure, direction,
        AVG(delta_cdg1)            AS delta_cdg1_min,
        MAX(delta_cdg1)          AS max_delta_cdg1
    FROM cdg1 GROUP BY 1, 2
),
agg_vert_galant AS (
    SELECT heure_vert_galant AS heure, direction,
        AVG(delta_vert_galant)     AS delta_vert_galant_min,
        MAX(delta_vert_galant)          AS max_delta_vert_galant
    FROM vert_galant GROUP BY 1, 2
)

-- Assemblage final
SELECT
    COALESCE(s.heure, a.heure, b.heure, c.heure, au.heure, cdg.heure, vg.heure) AS heure,
    COALESCE(s.direction, a.direction, b.direction, c.direction, au.direction, cdg.direction, vg.direction) AS direction,
    s.delta_sceaux_min,          s.max_delta_sceaux,
    a.delta_antony_min,          a.max_delta_antony,
    b.delta_bourg_la_reine_min,  b.max_delta_bourg_la_reine,
    c.delta_chatelet_min,        c.max_delta_chatelet,
    au.delta_aulnay_min,         au.max_delta_aulnay,
    cdg.delta_cdg1_min,          cdg.max_delta_cdg1,
    vg.delta_vert_galant_min,    vg.max_delta_vert_galant
FROM agg_chatelet  c
    FULL OUTER JOIN agg_antony       a   ON s.heure = a.heure   AND s.direction = a.direction
    FULL OUTER JOIN agg_bourg_la_reine b ON s.heure = b.heure   AND s.direction = b.direction
    FULL OUTER JOIN agg_sceaux     s     ON s.heure = c.heure   AND s.direction = c.direction
    FULL OUTER JOIN agg_aulnay       au  ON s.heure = au.heure  AND s.direction = au.direction
    FULL OUTER JOIN agg_cdg1         cdg ON s.heure = cdg.heure AND s.direction = cdg.direction
    FULL OUTER JOIN agg_vert_galant  vg  ON s.heure = vg.heure  AND s.direction = vg.direction
ORDER BY heure, direction
;