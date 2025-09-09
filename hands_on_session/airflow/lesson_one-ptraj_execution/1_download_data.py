from __future__ import annotations

import json
import os
import pathlib
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# py4lexis for dataset download
try:
    from py4lexis.core.session import LexisSessionOffline
    from py4lexis.core.lexis_irods import iRODS
except Exception:
    LexisSessionOffline = None  # type: ignore
    iRODS = None  # type: ignore

# Optional HDF5 transpose (small demo)
try:
    from ida4sims.transpose import transpose_trajectory_hdf5
    from MDAnalysis import Universe
except Exception:
    transpose_trajectory_hdf5 = None  # type: ignore
    Universe = None  # type: ignore

DOWNLOAD_ROOT = "/opt/airflow/data/downloads"
OUTPUT_ROOT = "/opt/airflow/data/outputs"


def _resolve_dataset_id(conf: dict, params: dict) -> str:
    # Prefer Airflow Variable, then DAG conf, then params
    var_id = Variable.get("UNIFIED_DATASET_ID", default_var="").strip()
    if var_id:
        return var_id
    cid = (conf.get("dataset_id") or params.get("dataset_id") or "").strip()
    return cid


def download_dataset(**context) -> None:
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    dataset_id = _resolve_dataset_id(conf, context["params"])
    if not dataset_id:
        raise ValueError("No dataset_id provided. Set Variable UNIFIED_DATASET_ID or pass it in conf/params.")

    if LexisSessionOffline is None or iRODS is None:
        raise RuntimeError("py4lexis is not available in this environment")

    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    out_dir = os.path.join(DOWNLOAD_ROOT, dataset_id)
    context["ti"].xcom_push(key="dataset_dir", value=out_dir)

    if os.path.exists(out_dir):
        print(f"Dataset {dataset_id} already present at {out_dir}; skipping download.")
        return

    refresh_token = Variable.get("lexis_refresh_token")
    session = LexisSessionOffline(refresh_token=refresh_token)
    irods = iRODS(session=session, suppress_print=False)

    print(f"Downloading dataset {dataset_id} to {DOWNLOAD_ROOT}...")
    irods.download_dataset_as_directory(access="project", project="exa4mind_wp4", dataset_id=dataset_id, local_directorypath=DOWNLOAD_ROOT)
    print("Download finished.")


def _pick_one(files: list[str]) -> str:
    files = sorted(files)
    if not files:
        return ""
    return files[0]


def resolve_inputs(**context) -> None:
    dataset_dir = context["ti"].xcom_pull(task_ids="download_dataset", key="dataset_dir")
    if not dataset_dir:
        raise RuntimeError("dataset_dir not set")

    tops: list[str] = []
    trajs: list[str] = []
    ptraj_ins: list[str] = []

    for root, _dirs, files in os.walk(dataset_dir):
        for fn in files:
            p = os.path.join(root, fn)
            lower = fn.lower()
            if lower.endswith(('.top', '.prmtop')):
                tops.append(p)
            elif lower.endswith(('.traj', '.nc', '.mdcrd', '.crd')):
                trajs.append(p)
            elif lower == 'nmr_unoe_ptraj.in':
                ptraj_ins.append(p)

    top = _pick_one(tops)
    traj = _pick_one(trajs)
    ptraj_in = _pick_one(ptraj_ins)

    if not (top and traj and ptraj_in):
        raise FileNotFoundError(
            f"Missing inputs in {dataset_dir}. Found top={bool(top)}, traj={bool(traj)}, ptraj_in={bool(ptraj_in)}"
        )

    print(f"Resolved: top={top}\ntraj={traj}\ninput={ptraj_in}")

    out_dir = os.path.join(OUTPUT_ROOT, context["run_id"])  # keep outputs separate by run
    os.makedirs(out_dir, exist_ok=True)

    ti = context["ti"]
    ti.xcom_push(key="dataset_dir", value=dataset_dir)
    ti.xcom_push(key="top", value=top)
    ti.xcom_push(key="traj", value=traj)
    ti.xcom_push(key="ptraj_in", value=ptraj_in)
    ti.xcom_push(key="out_dir", value=out_dir)


def write_run_summary(**context) -> None:
    run_id = context["run_id"]
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    params = context["params"]

    dataset_id = _resolve_dataset_id(conf, params)
    dataset_dir = context["ti"].xcom_pull(task_ids="resolve_inputs", key="dataset_dir")
    top = context["ti"].xcom_pull(task_ids="resolve_inputs", key="top")
    traj = context["ti"].xcom_pull(task_ids="resolve_inputs", key="traj")
    ptraj_in = context["ti"].xcom_pull(task_ids="resolve_inputs", key="ptraj_in")

    out_file = os.path.join(dataset_dir or '', "md-dist-4-unoes.dat")

    summaries_root = "/opt/airflow/data/run_summaries"
    os.makedirs(summaries_root, exist_ok=True)
    out_json = os.path.join(summaries_root, f"{run_id}.json")

    summary = {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "dataset_dir": dataset_dir,
        "topology": top,
        "trajectory": traj,
        "ptraj_input": ptraj_in,
        "output_present": os.path.exists(out_file),
        "output_path": out_file,
        "note": "Outputs are written in the dataset folder by design (easy to inspect and cleanup).",
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"Wrote summary -> {out_json}")


def transpose_small_hdf5(**context) -> None:
    # Optional small HDF5 demo; controlled by param do_small_hdf5
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    params = context.get("params") or {}
    if not bool(conf.get("do_small_hdf5", params.get("do_small_hdf5", True))):
        print("do_small_hdf5=false; skipping HDF5 transposition.")
        return
    if Universe is None or transpose_trajectory_hdf5 is None:
        print("MDAnalysis/ida4sims not available; skipping HDF5 transposition.")
        return
    top = context["ti"].xcom_pull(task_ids="resolve_inputs", key="top")
    traj = context["ti"].xcom_pull(task_ids="resolve_inputs", key="traj")
    if not top or not traj:
        print("Inputs not resolved; skipping HDF5 transposition.")
        return
    out_dir = os.path.join(OUTPUT_ROOT, context["run_id"], "hdf5")
    os.makedirs(out_dir, exist_ok=True)
    u = Universe(top, traj)
    h5_path = transpose_trajectory_hdf5(u, out_dir, f"{context['run_id']}_small.h5", do_partial_processing=True)
    print(f"Wrote small HDF5 -> {h5_path}")


with DAG(
    dag_id="unified_simulation",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    params={
        "dataset_id": "",
    },
    tags=["simulation", "workshop"],
) as dag:
    fetch_data = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    )

    fetch_data
