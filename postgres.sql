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


with init_reel as (select t.* ,
MAX(date_observation) OVER () as date_max
from horaires_reels_trv t),
reel as (SELECT *
FROM init_reel
WHERE date_observation = date_max),
init_th as (select t.* ,
MAX(date_observation) OVER () as date_max
from horaires_theoriques_trv t),
th as (SELECT *
FROM init_th
WHERE date_observation = date_max)
SELECT *
FROM th
UNION ALL
SELECT *
FROM reel
ORDER BY mission