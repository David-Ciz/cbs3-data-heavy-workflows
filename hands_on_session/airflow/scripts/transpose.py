import os

import h5py

def transpose_trajectory_hdf5(u, out_dir_path, hdf5_name):
    """
        Takes a trajectory, transposes it, splits it into chunks based on chunk_size, and saves it into a csv file in
        geometry format. Could still be possibly sped up by using pool of workers
        :param u: MDA Universe containing the trajectory of atoms.
        :param out_dir_path: hdf5 is saved to this path.
        :param hdf5_name: hdf5 is saved with this name.
        :return: str path to chunks_geometry.csv file
    """
    atoms = u.select_atoms("all")
    hdf_out_path = os.path.join(out_dir_path, hdf5_name)
    trajectory_length = len(u.trajectory)

    logging.info(f"transposing the trajectory and saving it to {hdf_out_path} as an HDF5 file")
    # Create the HDF5 file and datasets for each atom
    with h5py.File(hdf_out_path, 'w') as hf:
        # Number of runs (assuming 1 for simplicity)
        for run_id in range(1):
            run_group = hf.create_group(f'run{run_id}')

            # Preallocate datasets for each atom
            atom_datasets = [run_group.create_dataset(f'atom{atom_id}', (trajectory_length, 3), dtype='float32') for
                             atom_id in range(len(atoms))]

        # Fill the datasets with snapshots
        for i, ts in enumerate(tqdm(u.trajectory)):
            positions = atoms.positions
            for atom_id, atom_dataset in enumerate(atom_datasets):
                atom_dataset[i] = positions[atom_id]
    return hdf_out_path