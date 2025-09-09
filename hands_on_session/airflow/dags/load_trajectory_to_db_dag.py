from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable

# Robust import: Airflow only adds the DAGs folder to sys.path. If 'scripts' is a sibling
# of 'dags', resolve it dynamically so parsing works in scheduler/webserver.
try:
    from scripts.load_traj_to_db import load_traj_to_db as _load_traj_to_db
except ModuleNotFoundError:  # scheduler-safe fallback
    import sys
    from pathlib import Path

    scripts_dir = Path(__file__).resolve().parents[1] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.append(str(scripts_dir))
    from load_traj_to_db import load_traj_to_db as _load_traj_to_db

# Robust import for distance analysis
try:
    from scripts.distance_analysis import (
        upsert_specs_and_compute_distances as _compute_distances_from_spec,
    )
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    scripts_dir = Path(__file__).resolve().parents[1] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.append(str(scripts_dir))
    from distance_analysis import (
        upsert_specs_and_compute_distances as _compute_distances_from_spec,
    )


@dag(
    dag_id="load_trajectory_to_db",
    start_date=pendulum.datetime(2025, 9, 3, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["simulation", "database"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def load_trajectory_to_db():
    # You can manage these via Airflow Variables or set static defaults.
    topology_path = Variable.get(
        "traj_topology_path",
        default_var="/opt/airflow/data/rerun_10us-traj/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top",
    )
    trajectory_path = Variable.get(
        "traj_trajectory_path",
        default_var="/opt/airflow/data/rerun_10us-traj/caau_gHBfix21-tHBfix20_NBfix-both-0BPhs_rep1-10us_strip.traj",
    )
    postgres_conn_id = Variable.get("simulation_db_conn_id", default_var="simulation_db")
    distance_spec_path = Variable.get(
        "distance_spec_path",
        default_var="/opt/airflow/scripts/nmr_unoe_ptraj.in",
    )

    @task(execution_timeout=timedelta(hours=24), pool="default")
    def run_loader():
        _load_traj_to_db(
            topology_path=topology_path,
            trajectory_path=trajectory_path,
            postgres_conn_id=postgres_conn_id,
            coord_batch_size=10_000,
        )

    @task(execution_timeout=timedelta(hours=6), pool="default")
    def run_distance_analysis():
        _compute_distances_from_spec(
            spec_path=distance_spec_path,
            postgres_conn_id=postgres_conn_id,
        )

    load = run_loader()
    dist = run_distance_analysis()
    load >> dist


dag = load_trajectory_to_db()