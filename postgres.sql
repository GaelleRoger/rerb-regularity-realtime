WITH init as (select type_mission, gare_depart, nb_arrets_desservis,
MAX(nb_arrets_desservis) OVER (PARTITION BY type_mission) as nb_arrets_max
from horaires_theoriques)

SELECT distinct type_mission, gare_depart
from init
WHERE nb_arrets_desservis = nb_arrets_max