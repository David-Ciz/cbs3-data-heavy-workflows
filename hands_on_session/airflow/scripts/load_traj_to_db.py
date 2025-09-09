import logging
from typing import Iterable, List, Tuple

# Defer heavy imports to function runtime to avoid Airflow parse-time failures
from psycopg2.extras import execute_values


log = logging.getLogger(__name__)


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS atoms (
    atom_index INTEGER PRIMARY KEY,
    atom_name TEXT NOT NULL,
    residue_id INTEGER NOT NULL,
    residue_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frames (
    frame_index INTEGER PRIMARY KEY,
    timestamp DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS coordinates (
    frame_index INTEGER REFERENCES frames(frame_index) ON DELETE CASCADE,
    atom_index INTEGER REFERENCES atoms(atom_index) ON DELETE CASCADE,
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    z DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (frame_index, atom_index)
);
"""


def _chunks(seq: List[Tuple], size: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def load_traj_to_db(
    topology_path: str,
    trajectory_path: str,
    postgres_conn_id: str = "simulation_db",
    coord_batch_size: int = 10_000,
) -> None:
    """
    Load a trajectory into Postgres with idempotent upserts and batched inserts.

    - Uses atom.index (0-based) as stable primary key for atoms.
    - Uses frame_index (0-based) as primary key for frames.
    - coordinates primary key is (frame_index, atom_index).
    - ON CONFLICT DO NOTHING makes the task safe to re-run.
    """
    # Import heavy/optional deps at runtime
    import MDAnalysis as mda
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    log.info("Loading trajectory. Topology=%s, Trajectory=%s", topology_path, trajectory_path)

    # Build universe (this is the CPU/memory-heavy part)
    u = mda.Universe(topology_path, trajectory_path)

    # Connect via Airflow's PostgresHook
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False  # single transaction for DDL + inserts
    try:
        with conn, conn.cursor() as cur:
            # DDL
            cur.execute(CREATE_TABLES_SQL)

            # Upsert atoms once
            atom_rows = [(a.index, a.name, a.resid, a.resname) for a in u.atoms]
            log.info("Upserting %d atoms", len(atom_rows))
            execute_values(
                cur,
                """
                INSERT INTO atoms (atom_index, atom_name, residue_id, residue_name)
                VALUES %s
                ON CONFLICT (atom_index) DO NOTHING
                """,
                atom_rows,
                page_size=10_000,
            )

            # Iterate frames: insert frame and its coordinates in batches
            for frame_idx, ts in enumerate(u.trajectory):
                # Upsert frame
                cur.execute(
                    """
                    INSERT INTO frames (frame_index, timestamp)
                    VALUES (%s, %s)
                    ON CONFLICT (frame_index) DO NOTHING
                    """,
                    (frame_idx, float(getattr(ts, "time", frame_idx))),
                )

                # Coordinates for this frame (batched)
                coords = u.atoms.positions  # shape (n_atoms, 3)
                coord_rows = [
                    (frame_idx, a.index, float(coords[i, 0]), float(coords[i, 1]), float(coords[i, 2]))
                    for i, a in enumerate(u.atoms)
                ]

                for batch in _chunks(coord_rows, coord_batch_size):
                    execute_values(
                        cur,
                        """
                        INSERT INTO coordinates (frame_index, atom_index, x, y, z)
                        VALUES %s
                        ON CONFLICT (frame_index, atom_index) DO NOTHING
                        """,
                        batch,
                        page_size=coord_batch_size,
                    )

        conn.commit()
        log.info("Trajectory load completed successfully.")
    except Exception as e:
        conn.rollback()
        log.exception("Failed to load trajectory: %s", e)
        raise
    finally:
        conn.close()