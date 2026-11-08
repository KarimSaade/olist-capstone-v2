-- Runs once, the first time the Postgres container starts with an empty volume.
-- The "airflow" database (Airflow metadata) is created by POSTGRES_DB.
-- "olist" holds the Task 1 tables and the ETL output from the Airflow DAG.
CREATE DATABASE olist;
