from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from scripts.load_traj_to_db import load_traj_to_db

with DAG(
    dag_id="load_trajectory_to_db",
    start_date=pendulum.datetime(2025, 9, 3, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["simulation", "database"],
) as dag:
    load_trajectory_task = PythonOperator(
        task_id="load_trajectory",
        python_callable=load_traj_to_db,
    )
