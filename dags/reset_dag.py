"""
DAG Reset - Réinitialisation quotidienne des tables Postgres
Ce DAG supprime toutes les tables du schéma public (sauf hist_moyenne_*) à 1h59 du matin.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PIPELINE_PATH = os.path.join(PROJECT_ROOT, 'src')

default_args = {
    'owner': 'gaelle-roger',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'reset-rerb-realtime',
    default_args=default_args,
    description='Réinitialisation quotidienne des tables Postgres',
    schedule_interval='1 1 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['postgres', 'reset', 'rerb'],
)

reset_task = BashOperator(
    task_id='reset_postgres',
    bash_command=(
        f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
        f'cd {PIPELINE_PATH} && '
        'python3 reset_postgres.py'
    ),
    dag=dag,
)
