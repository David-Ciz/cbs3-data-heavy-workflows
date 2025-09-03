import psycopg2
import MDAnalysis as mda
import os

def load_traj_to_db():
    # Connect to the database
    conn = psycopg2.connect(
        dbname="simulation",
        user="user",
        password="password",
        host="simulation_db",
        port="5432"
    )
    cur = conn.cursor()

    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atoms (
            atom_id SERIAL PRIMARY KEY,
            atom_name TEXT,
            residue_id INTEGER,
            residue_name TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS frames (
            frame_id SERIAL PRIMARY KEY,
            timestamp FLOAT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS coordinates (
            frame_id INTEGER REFERENCES frames(frame_id),
            atom_id INTEGER REFERENCES atoms(atom_id),
            x FLOAT,
            y FLOAT,
            z FLOAT,
            PRIMARY KEY (frame_id, atom_id)
        );
    """)

    # Load the trajectory
    universe = mda.Universe(
        "/opt/airflow/data/rerun_10us-traj/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top",
        "/opt/airflow/data/rerun_10us-traj/caau_gHBfix21-tHBfix20_NBfix-both-0BPhs_rep1-10us_strip.traj"
    )

    # Populate atoms table
    for atom in universe.atoms:
        cur.execute(
            "INSERT INTO atoms (atom_id, atom_name, residue_id, residue_name) VALUES (%s, %s, %s, %s)",
            (atom.id, atom.name, atom.resid, atom.resname)
        )

    # Populate frames and coordinates tables
    for ts in universe.trajectory:
        cur.execute("INSERT INTO frames (timestamp) VALUES (%s) RETURNING frame_id", (ts.time,))
        frame_id = cur.fetchone()[0]
        for atom in universe.atoms:
            cur.execute(
                "INSERT INTO coordinates (frame_id, atom_id, x, y, z) VALUES (%s, %s, %s, %s, %s)",
                (frame_id, atom.id, atom.position[0], atom.position[1], atom.position[2])
            )

    # Commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_traj_to_db()
