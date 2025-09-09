from __future__ import annotations

import json
import os
import time
import logging
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Optional deps (available inside the Airflow image)
try:
    from py4lexis.core.session import LexisSessionOffline
    from py4lexis.core.lexis_irods import iRODS
except Exception:  # keep module importable outside container
    LexisSessionOffline = None  # type: ignore
    iRODS = None  # type: ignore

try:
    import h5py
    from MDAnalysis import Universe
except Exception:
    h5py = None  # type: ignore
    Universe = None  # type: ignore

DOWNLOAD_ROOT = "/opt/airflow/data/downloads"
OUTPUT_ROOT = "/opt/airflow/data/outputs"
SUMMARY_ROOT = "/opt/airflow/data/run_summaries"


def _resolve_dataset_id(conf: dict, params: dict) -> str:
    # Prefer Variable, then conf, then params
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

    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    out_dir = os.path.join(DOWNLOAD_ROOT, dataset_id)
    context["ti"].xcom_push(key="dataset_dir", value=out_dir)

    if os.path.exists(out_dir):
        print(f"Dataset {dataset_id} already present at {out_dir}; skipping download.")
        return

    if LexisSessionOffline is None or iRODS is None:
        raise RuntimeError("py4lexis is not available in this environment")

    refresh_token = Variable.get("lexis_refresh_token")
    session = LexisSessionOffline(refresh_token=refresh_token)
    irods = iRODS(session=session, suppress_print=False)

    print(f"Downloading dataset {dataset_id} to {DOWNLOAD_ROOT}...")
    irods.download_dataset_as_directory(access="project", project="exa4mind_wp4", dataset_id=dataset_id, local_directorypath=DOWNLOAD_ROOT)
    print("Download finished.")


def _pick_one(files: list[str]) -> str:
    files = sorted(files)
    return files[0] if files else ""


def resolve_inputs(**context) -> None:
    dataset_dir = context["ti"].xcom_pull(task_ids="download_dataset", key="dataset_dir")
    if not dataset_dir:
        raise RuntimeError("dataset_dir not set")

    tops: list[str] = []
    trajs: list[str] = []

    for root, _dirs, files in os.walk(dataset_dir):
        for fn in files:
            p = os.path.join(root, fn)
            lower = fn.lower()
            if lower.endswith((".top", ".prmtop")):
                tops.append(p)
            elif lower.endswith((".traj", ".nc", ".mdcrd", ".crd")):
                trajs.append(p)

    top = _pick_one(tops)
    traj = _pick_one(trajs)

    if not (top and traj):
        raise FileNotFoundError(
            f"Missing inputs in {dataset_dir}. Found top={bool(top)}, traj={bool(traj)}"
        )

    print(f"Resolved: top={top}\ntraj={traj}")

    out_dir = os.path.join(OUTPUT_ROOT, context["run_id"], "transposition")
    os.makedirs(out_dir, exist_ok=True)

    ti = context["ti"]
    ti.xcom_push(key="dataset_dir", value=dataset_dir)
    ti.xcom_push(key="top", value=top)
    ti.xcom_push(key="traj", value=traj)
    ti.xcom_push(key="out_dir", value=out_dir)


def _open_universe(top: str, traj: str) -> "Universe":
    if Universe is None:
        raise RuntimeError("MDAnalysis not available in this environment")
    return Universe(top, traj)


def transpose_prealloc(**context) -> None:
    params = (context.get("dag_run") and context["dag_run"].conf) or context["params"]
    frame_limit = int(params.get("frame_limit", 1000) or 0)

    top = context["ti"].xcom_pull(task_ids="resolve_inputs", key="top")
    traj = context["ti"].xcom_pull(task_ids="resolve_inputs", key="traj")
    out_dir = context["ti"].xcom_pull(task_ids="resolve_inputs", key="out_dir")

    if h5py is None:
        raise RuntimeError("h5py not available in this environment")

    u = _open_universe(top, traj)
    atoms = u.select_atoms("all")
    n_atoms = len(atoms)
    n_frames = len(u.trajectory)
    if frame_limit and frame_limit < n_frames:
        n_frames = frame_limit

    out_path = os.path.join(out_dir, "prealloc.h5")
    t0 = time.perf_counter()

    logging.info(f"[prealloc] writing {n_frames} frames, {n_atoms} atoms -> {out_path}")
    with h5py.File(out_path, "w") as hf:
        run_group = hf.create_group("run0")
        dsets = [
            run_group.create_dataset(f"atom{aid}", (n_frames, 3), dtype="float32")
            for aid in range(n_atoms)
        ]
        # fill
        for i, ts in enumerate(u.trajectory):
            if i >= n_frames:
                break
            pos = atoms.positions  # (n_atoms, 3), float64
            # write per atom row i
            for aid in range(n_atoms):
                dsets[aid][i] = pos[aid]

    dt = time.perf_counter() - t0
    context["ti"].xcom_push(key="hdf5_path", value=out_path)
    context["ti"].xcom_push(key="prealloc_seconds", value=dt)
    print(f"[prealloc] done in {dt:.2f}s -> {out_path}")


def transpose_no_prealloc(**context) -> None:
    params = (context.get("dag_run") and context["dag_run"].conf) or context["params"]
    frame_limit = int(params.get("frame_limit", 1000) or 0)

    top = context["ti"].xcom_pull(task_ids="resolve_inputs", key="top")
    traj = context["ti"].xcom_pull(task_ids="resolve_inputs", key="traj")
    out_dir = context["ti"].xcom_pull(task_ids="resolve_inputs", key="out_dir")

    if h5py is None:
        raise RuntimeError("h5py not available in this environment")

    u = _open_universe(top, traj)
    atoms = u.select_atoms("all")
    n_atoms = len(atoms)
    n_frames = len(u.trajectory)
    if frame_limit and frame_limit < n_frames:
        n_frames = frame_limit

    out_path = os.path.join(out_dir, "no_prealloc.h5")
    t0 = time.perf_counter()

    logging.info(f"[no_prealloc] writing {n_frames} frames, {n_atoms} atoms -> {out_path}")
    with h5py.File(out_path, "w") as hf:
        run_group = hf.create_group("run0")
        # create resizable datasets per atom (start empty), small chunks
        dsets = [
            run_group.create_dataset(
                f"atom{aid}",
                shape=(0, 3),
                maxshape=(None, 3),
                dtype="float32",
                chunks=(1024, 3),
            )
            for aid in range(n_atoms)
        ]
        # fill with per-frame resizes
        for i, ts in enumerate(u.trajectory):
            if i >= n_frames:
                break
            pos = atoms.positions
            for aid in range(n_atoms):
                dset = dsets[aid]
                dset.resize((i + 1, 3))
                dset[i] = pos[aid]

    dt = time.perf_counter() - t0
    context["ti"].xcom_push(key="no_prealloc_path", value=out_path)
    context["ti"].xcom_push(key="no_prealloc_seconds", value=dt)
    print(f"[no_prealloc] done in {dt:.2f}s -> {out_path}")


def summarize(**context) -> None:
    run_id = context["run_id"]
    os.makedirs(SUMMARY_ROOT, exist_ok=True)
    out_json = os.path.join(SUMMARY_ROOT, f"{run_id}_transpose.json")

    pre_path = context["ti"].xcom_pull(task_ids="transpose_prealloc", key="prealloc_path")
    pre_sec = context["ti"].xcom_pull(task_ids="transpose_prealloc", key="prealloc_seconds")
    nopath = context["ti"].xcom_pull(task_ids="transpose_no_prealloc", key="no_prealloc_path")
    nosec = context["ti"].xcom_pull(task_ids="transpose_no_prealloc", key="no_prealloc_seconds")

    data = {
        "prealloc": {"path": pre_path, "seconds": float(pre_sec) if pre_sec is not None else None},
        "no_prealloc": {"path": nopath, "seconds": float(nosec) if nosec is not None else None},
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote summary -> {out_json}")


with DAG(
    dag_id="transposition_demo",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    params={
        # Optional: override; prefer Variable UNIFIED_DATASET_ID
        "dataset_id": "",
        # Limit frames for fast demo
        "frame_limit": 1000,
    },
    tags=["simulation", "workshop", "hdf5"],
) as dag:

    fetch = PythonOperator(task_id="download_dataset", python_callable=download_dataset)
    resolve = PythonOperator(task_id="resolve_inputs", python_callable=resolve_inputs)

    prealloc = PythonOperator(task_id="transpose_prealloc", python_callable=transpose_prealloc)
    no_prealloc = PythonOperator(task_id="transpose_no_prealloc", python_callable=transpose_no_prealloc)

    summary = PythonOperator(task_id="summarize", python_callable=summarize)

    fetch >> resolve >> prealloc >> no_prealloc >> summary

