"""
DAG Pipeline - Orchestration des scripts RERB
Ce DAG orchestre l'exécution des scripts situés dans PROJET/src/
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin vers le dossier src pour importer les scripts
# Ajuster le chemin selon votre structure de dossiers
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PIPELINE_PATH = os.path.join(PROJECT_ROOT, 'src', 'pipeline')
sys.path.insert(0, PIPELINE_PATH)

# Configuration par défaut du DAG
default_args = {
    'owner': 'gaelle-roger',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Définition du DAG
dag = DAG(
    'regularite-rerb-realtime',
    default_args=default_args,
    description='Pipeline ETL',
    schedule_interval='*/5 0,3-23 * * *',
    start_date=datetime(2026, 3, 30),
    catchup=False,
    tags=['postgres', 'pipeline', 'rerb'],
)


extract_task_bash = BashOperator(
    task_id='extract_data',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 extract_horaires.py'
    ),
    dag=dag,
)

load_task_bash = BashOperator(
    task_id='load_data',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 load_postgres.py'
    ),
    dag=dag,
)

referentiel_task_bash = BashOperator(
    task_id='creation_referentiel',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 creation_table_ref.py'
    ),
    dag=dag,
)

ecarts_task_bash = BashOperator(
    task_id='calcul_ecarts_horaires',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 calcul_ecarts_horaires.py'
    ),
    dag=dag,
)

allongement_task_bash = BashOperator(
    task_id='calcul_allongement_parcours',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 calcul_allongement_parcours.py'
    ),
    dag=dag,
)

regularite_task_bash = BashOperator(
    task_id='calcul_regularite',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 calcul_regularite.py'
    ),
    dag=dag,
)

extract_task_bash >> load_task_bash >> referentiel_task_bash >> [regularite_task_bash, ecarts_task_bash]

ecarts_task_bash >> allongement_task_bash