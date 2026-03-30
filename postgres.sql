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


with th as(select mission, 'theorique' as type_horaire, luxembourg, nb_arrets_desservis, date_observation,
MAX(date_observation) OVER () as date_max
from horaires_theoriques_trv
where luxembourg is not null),
reel as (select mission, 'reel' as type_horaire, luxembourg, nb_arrets_desservis, date_observation,
MAX(date_observation) OVER () as date_max
from horaires_reels_trv
where luxembourg is not null),
concat as (select * from th WHERE date_observation = date_max
UNION ALL
select * from reel WHERE date_observation = date_max),
compact as (select mission, type_horaire, luxembourg 
from
concat c
INNER JOIN referentiel_missions r
ON SUBSTRING(c.mission,1,4) = r.code_mission
WHERE nb_arrets_desservis < nb_arrets_mission)
SELECT
    mission,
    ROUND(
        EXTRACT(EPOCH FROM (
            MAX(luxembourg::timestamptz) FILTER (WHERE type_horaire = 'reel')
          - MAX(luxembourg::timestamptz) FILTER (WHERE type_horaire = 'theorique')
        )) / 60
    )::INTEGER AS luxembourg
FROM compact
GROUP BY mission
ORDER BY mission;