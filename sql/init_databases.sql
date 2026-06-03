-- Runs once on first postgres container start.
-- Creates the pipeline warehouse database alongside the Airflow metadata DB.
SELECT 'CREATE DATABASE pipeline_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pipeline_db')\gexec
