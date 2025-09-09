import logging
import re
from pathlib import Path
from typing import Iterable, List, Tuple

log = logging.getLogger(__name__)

DDL_SQL = """
CREATE TABLE IF NOT EXISTS distance_specs (
    name TEXT PRIMARY KEY,
    resid1 INTEGER NOT NULL,
    atom_name1 TEXT NOT NULL,
    resid2 INTEGER NOT NULL,
    atom_name2 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS distances (
    name TEXT REFERENCES distance_specs(name) ON DELETE CASCADE,
    frame_index INTEGER NOT NULL,
    distance DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (name, frame_index)
);

CREATE INDEX IF NOT EXISTS idx_distances_name ON distances(name);
CREATE INDEX IF NOT EXISTS idx_coords_atom_frame ON coordinates(atom_index, frame_index);
"""

INSERT_SPECS_SQL = """
INSERT INTO distance_specs (name, resid1, atom_name1, resid2, atom_name2)
VALUES %s
ON CONFLICT (name) DO UPDATE SET
    resid1 = EXCLUDED.resid1,
    atom_name1 = EXCLUDED.atom_name1,
    resid2 = EXCLUDED.resid2,
    atom_name2 = EXCLUDED.atom_name2;
"""

COMPUTE_DISTANCES_SQL = """
INSERT INTO distances (name, frame_index, distance)
SELECT s.name,
       c1.frame_index,
       sqrt(power(c1.x - c2.x, 2) + power(c1.y - c2.y, 2) + power(c1.z - c2.z, 2)) AS distance
FROM distance_specs s
JOIN atoms a1
  ON a1.residue_id = s.resid1 AND a1.atom_name = s.atom_name1
JOIN atoms a2
  ON a2.residue_id = s.resid2 AND a2.atom_name = s.atom_name2
JOIN coordinates c1
  ON c1.atom_index = a1.atom_index
JOIN coordinates c2
  ON c2.atom_index = a2.atom_index AND c2.frame_index = c1.frame_index
ON CONFLICT (name, frame_index) DO UPDATE SET distance = EXCLUDED.distance;
"""

SPEC_LINE_RE = re.compile(
    r"^\s*distance\s+(?P<name>\S+)\s+:(?P<res1>\d+)@(?P<atom1>[^\s]+)\s+:(?P<res2>\d+)@(?P<atom2>[^\s]+)\s*$",
    re.IGNORECASE,
)


def parse_ptraj_distance_specs(spec_path: str | Path) -> List[Tuple[str, int, str, int, str]]:
    path = Path(spec_path)
    if not path.exists():
        raise FileNotFoundError(f"Spec file not found: {path}")
    specs: List[Tuple[str, int, str, int, str]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith(("#", "//")):
                continue
            m = SPEC_LINE_RE.match(line)
            if not m:
                # ignore lines like 'trajin', 'run', 'writedata', etc.
                if line.lower().startswith(("trajin", "run", "writedata")):
                    continue
                log.debug("Skipping unrecognized line %d: %s", line_no, line)
                continue
            name = m.group("name")
            resid1 = int(m.group("res1"))
            atom1 = m.group("atom1")
            resid2 = int(m.group("res2"))
            atom2 = m.group("atom2")
            specs.append((name, resid1, atom1, resid2, atom2))
    if not specs:
        raise ValueError(f"No distance specs found in {path}")
    return specs


def upsert_specs_and_compute_distances(
    spec_path: str | Path,
    postgres_conn_id: str = "simulation_db",
) -> None:
    """Parse a ptraj-style distance spec file, upsert specs, and compute/store distances.

    Creates tables distance_specs and distances if they do not exist, then fills distances
    for all frames and specs in one set-based SQL statement.
    """
    from psycopg2.extras import execute_values
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    specs = parse_ptraj_distance_specs(spec_path)
    log.info("Parsed %d specs from %s", len(specs), spec_path)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL_SQL)

            execute_values(cur, INSERT_SPECS_SQL, specs, page_size=1000)
            log.info("Upserted %d specs into distance_specs", len(specs))

            cur.execute(COMPUTE_DISTANCES_SQL)
            log.info("Computed distances for all specs across frames")
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to compute distances from specs: %s", spec_path)
        raise
    finally:
        conn.close()


def compute_distances_for_existing_specs(postgres_conn_id: str = "simulation_db") -> None:
    """Compute distances for all specs already present in distance_specs."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL_SQL)
            cur.execute(COMPUTE_DISTANCES_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to compute distances for existing specs")
        raise
    finally:
        conn.close()

