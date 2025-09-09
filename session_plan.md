# Hands-On Session: Automating Data-Heavy Scientific Workflows (V2)

## 1. Session Overview

*   **Title:** Hands-On Exercise: Automatizing Data-Heavy Workflows with ADAMS4SIMS and Modern Data Engineering Tools.
*   **Duration:** Approx. 1 hour 45 minutes.
*   **Target Audience:** A mixed-skill audience with an interest in scientific computing and data analysis.
*   **Core Philosophy:** Learning through discovery, guided challenges, and controlled failure. This session focuses on understanding *why* we use certain tools, not just *how*.
*   **Environment:** All participants will connect to a single, shared server that hosts the required tools (Airflow, Database, Storage).

### Prerequisites & Setup

*   **Participant Tools:**
    *   Visual Studio Code (or preferred editor).
    *   PuTTY (or any SSH client) for connecting to the shared server.
*   **Shared Server Environment (Pre-configured):**
    *   A single, central Apache Airflow instance for workflow orchestration.
    *   A single, central PostgreSQL database for a data storage experiment.
    *   A shared file system for data and analysis scripts.

## 2. Learning Objectives

By the end of this session, participants will understand:

*   The limitations of manual, sequential data processing.
*   The benefits of workflow orchestration (parallelization, logging, error handling) using a tool like Airflow.
*   The critical impact of data storage choices on performance and scalability.
*   The dual problems of using a relational database for this task: **slow query performance** for analysis and **resource contention** under concurrent load.
*   The advantages of using scientific data formats like HDF5 on a shared file system.

## 3. Session Plan & Agenda

### Part 1: The Manual Process - Understanding the Pain (Approx. 20 mins)

1.  **Introduction:** Briefly introduce the ADAMS4SIMS platform and the goal: to run a molecular dynamics analysis.
2.  **The Manual Task:** Participants run a standard analysis using a tool like `ptraj`.
3.  **Observation:** Participants time the process and experience the manual effort. This sets the baseline pain point.

### Part 2: The Automated Workflow (Approx. 25 mins)

1.  **The "5-Minute" Challenge:** Propose the tongue-in-cheek challenge: "Let's design a system that can handle multiple requests, track everything, and resume from failure."
2.  **The Reveal - Airflow:** Introduce the shared, pre-existing Airflow instance as the solution. Guide participants to the Airflow UI in their browsers.
3.  **Guided Tour:** Briefly show the key components: DAGs, the grid view, logs.
4.  **Triggering the DAG:** Participants trigger their first analysis job via the Airflow UI.
5.  **The "Take-Home" Magic:** While it runs, *show* the `docker-compose.yml` file and explain that this entire powerful environment can be set up on their own laptops with a single `docker compose up` command, encouraging them to try it later.

### Part 3: Tackling Data & Infrastructure Challenges (Approx. 50 mins)

This part uses guided "fill-in-the-blank" exercises and "checkpoints" to ensure everyone can keep up.

1.  **Problem 1: The Data Overload**
    *   **Task:** "Transfer all datasets to your personal folder on the server for a large-batch analysis."
    *   **Expected Outcome:** Participants run out of their user-specific disk quota.
    *   **Lesson:** The need for a centralized, scalable storage solution.

2.  **Problem 2: The Shared Database Bottleneck**
    *   **Task:** "Let's use the central database. Everyone, run this script to load your data." The script will have a small blank to fill in (e.g., a table name).
    *   **Expected Outcome:**
        *   **Contention:** As everyone runs the script simultaneously, they will experience slowdowns, timeouts, or errors. The database becomes a bottleneck.
        *   **Slow Analysis:** The subsequent analysis query will be dramatically slower than the file-based approach.
    *   **Lesson:** Databases suffer from contention and are often inefficient for scientific analysis patterns.
    *   **Checkpoint:** After 15 minutes, provide a "finished" database state or move directly to the next step so no one is left behind.

3.  **Problem 3: The Right Tool for the Job - HDF5**
    *   **Task:** "Now, let's convert the data to HDF5 and place it in the shared storage area. Run the analysis again."
    *   **Expected Outcome:** A significant, contention-free performance speedup.
    *   **Lesson:** The power of using appropriate, file-based scientific data formats.

### Part 4: Wrap-up & Q&A (Approx. 10 mins)

*   **Recap:** Briefly summarize the key lessons learned: Manual vs. Orchestrated, DB vs. HDF5, Contention vs. Scalability.
*   **Q&A:** Open the floor for questions.

## 3a. Unified DAG: Download + Different-Sized Simulations (Proposed Hands‑On Flow)

This section aligns the live demo with what we’ve prepared in the repo so participants can see scale effects clearly without juggling many DAGs.

What we’ll run (in one DAG):
- Task order: download_dataset (optional) → run_normal → run_big_length → run_many_atoms → run_big_length_many_atoms (disabled by default) → write_run_summary.
- The DAG id: unified_simulation (hands_on_session/airflow/dags/unified_simulation_dag.py).
- Download step uses the same py4lexis pattern as simulation_preprocessing_dag (dataset_id param; leave empty to skip).
- Each run executes cpptraj with a different workload:
  - normal: 100k frames, 126 atoms (baseline)
  - big_length: 500k frames, 126 atoms (longer)
  - many_atoms: 100k frames, ~20k atoms via scaling (broader)
  - big_length_many_atoms: 500k frames, ~20k atoms (kept OFF by default for the workshop)
- Tracking: write_run_summary writes a JSON snapshot with what was enabled and whether the expected output appeared (under /opt/airflow/data/run_summaries).

How we’ll use it in the session:
- Show the unified DAG in the Airflow UI and briefly explain each task.
- Trigger once with defaults (largest OFF). Optionally provide a dataset_id to show the download task; otherwise it skips cleanly.
- While it runs, open task logs and the grid durations to compare runtimes across tasks.
- After completion, show the summary JSON and the output location. Call out that outputs share a working directory on purpose (a teachable constraint).

Why this is pedagogically useful:
- It demonstrates two scale axes separately (more frames vs. more atoms) and their impact on runtime and memory/IO.
- It centralizes observability in one place (one DAG), so participants don’t context‑switch.

Cleanup (intentionally deferred):
- We will NOT auto‑cleanup the downloaded data or outputs during the demo to make storage/ownership tangible.
- This is a good lesson: it surfaces the cost of “fire‑and‑forget” workflows and motivates a short conversation about data lifecycle policies (quotas, retention, and explicit cleanup tasks). We can end with a brief “Cleanup Challenge” and provide the ready‑made cleanup task afterwards.

Practical notes (what’s already prepared):
- We pre‑generated two datasets so the effects are visible without waiting on huge jobs:
  - big_length (500k frames, 126 atoms) wrote ~1.4 GB on disk.
  - many_atoms (100k frames, ~20k atoms) wrote ~17 GB on disk.
- The largest combo (500k × ~20k atoms) is disabled by default; enabling it should be a facilitator‑only option if resources allow.

Optional follow‑ups (time permitting):
- Show how to persist timing/params to a small table or JSON file per run for later comparisons.
- Add a cleanup task at the tail of the DAG (disabled by default) to demonstrate good hygiene after the lesson sinks in.

#### Control via Airflow Variables (per-run datasets)

- Set Airflow Variables (Admin → Variables) to auto-download and organize inputs:
  - UNIFIED_NORMAL_DATASET_ID
  - UNIFIED_BIG_LENGTH_DATASET_ID
  - UNIFIED_MANY_ATOMS_DATASET_ID
  - UNIFIED_BIG_LENGTH_MANY_ATOMS_DATASET_ID
- If a Variable is empty, the DAG uses local fallback files under /opt/airflow/data/rerun_10us-traj.
- Downloaded data lands in /opt/airflow/data/downloads (host: ./hands_on_session/data/downloads).
- Analysis outputs per task go to /opt/airflow/data/outputs/<run_id>/<run_key> (host: ./hands_on_session/data/outputs/...).
- This makes manual inspection and cleanup easy on the host, matching docker-compose volume mounts.
