from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ptraj_big_length",
    start_date=pendulum.datetime(2025, 9, 2, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["simulation"],
) as dag:
    run_analysis = BashOperator(
        task_id="run_small_analysis",
        bash_command="/tmp/cpptraj/bin/cpptraj -p /opt/airflow/data/rerun_10us-traj/big_length/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top -i /opt/airflow/scripts/big_length/nmr_unoe_ptraj.in",
    )
