-- Création de la base de données Airflow si elle n'existe pas déjà
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec
