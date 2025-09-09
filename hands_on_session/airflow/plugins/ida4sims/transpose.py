import logging
import os
import time
import paramiko

import h5py
import numpy as np
from MDAnalysis import Universe
from tqdm import tqdm

"""Contains transposition algorithm that is memory conscious and save the coordinates into a desired format.
 Several older variants are kept here to note what was explored. The constants defined here will be substituted later
 with extraction of this information from either files or the user"""

TEMPERATURE = 320
HILLS = "ParamHillsX"
RUN_ID = 1 # has to be unique and correspond to the correct run_id, currently just mock value


def transpose_trajectory_hdf5(u, out_dir_path, hdf5_name, do_partial_processing):
    """
        Takes a trajectory, transposes it, splits it into chunks based on chunk_size, and saves it into a csv file in
        geometry format. Could still be possibly sped up by using pool of workers
        :param u: MDA Universe containing the trajectory of atoms.
        :param out_dir_path: hdf5 is saved to this path.
        :param hdf5_name: hdf5 is saved with this name.
        :param do_partial_processing: if True, only first 1000 frames are processed, otherwise all frames
        :return: str path to chunks_geometry.csv file
    """
    atoms = u.select_atoms("all")
    hdf_out_path = os.path.join(out_dir_path, hdf5_name)
    if do_partial_processing:
        trajectory_slice = u.trajectory[:1000]
    else:
        trajectory_slice = u.trajectory
    trajectory_length = len(trajectory_slice)

    logging.info(f"transposing the trajectory and saving it to {hdf_out_path} as an HDF5 file")
    # Create the HDF5 file and datasets for each atom
    with h5py.File(hdf_out_path, 'w') as hf:
        # Number of runs (assuming 1 for simplicity)
        # TODO Spawn new runs for each of these loop objects
        # Preallocate datasets for each atom
        atom_datasets = [hf.create_dataset(f'atom{atom_id}', (trajectory_length, 3), dtype='float32') for
                             atom_id in range(len(atoms))]

        # Fill the datasets with snapshots
        # TODO paralelize this with several threads or processes
        for i, ts in enumerate(tqdm(trajectory_slice)):
            positions = atoms.positions
            for atom_id, atom_dataset in enumerate(atom_datasets):
                atom_dataset[i] = positions[atom_id]
    return hdf_out_path

#
# def transpose_trajectory_geometry_csv(u: Universe, out_dir_path: str | os.PathLike, start_id: int, ff_id: int, chunk_size: int = 1) -> str:
#     """
#     Takes a trajectory, transposes it, splits it into chunks based on chunk_size, and saves it into a csv file in
#     geometry format. Could still be possibly sped up by using pool of workers
#     :param u: MDA Universe containing the trajectory of atoms.
#     :param out_dir_path: csv is saved to this path.
#     :param chunk_size: size of chunks that should be saved. If simulation is replicated this value should depend on the
#      replica information. default set to 1 to mirror our replica data example
#      :param start_id: start of the database id sequence guaranteed to be reserved
#      :param ff_id: id of force field the chunk is in
#     :return: str path to chunks_geometry.csv file
#     """
#     chunk_edge = chunk_size
#     chunk_id = start_id
#     atoms = u.select_atoms("all")
#     csv_out_path = os.path.join(out_dir_path, "chunks_geometry.csv")
#     chunks = np.zeros((len(atoms), chunk_size, 3))
#     with open(csv_out_path, 'w') as f:
#         current_chunk_size = 0
#         for i, ts in enumerate(tqdm(u.trajectory[:100])):  # ts - timestep
#             chunks[:,i%chunk_size] = atoms.positions
#             current_chunk_size += 1
#             # last chunk size might be smaller than previous ones, save result when we get to the last frame
#             if current_chunk_size == chunk_edge or ts.time == len(u.trajectory) - 1:
#                 #geometry requires this specific format that is sensitive to spaces and delimiters
#                 if current_chunk_size == 1:
#                     f.write(f"{chunk_id};{ff_id};{current_chunk_size};MULTIPOINT(")
#                 else:
#                     f.write(f"{chunk_id};{ff_id};{current_chunk_size};MULTILINESTRING(")
#                 if current_chunk_size != chunk_size:
#                     chunks = chunks[:,:current_chunk_size]
#                 string_representation = ','.join('(' + ','.join(' '.join(f'{coordinates:.3f}' for coordinates in frame) for frame in atom) + ')' for atom in chunks) + ')\n'
#                 f.write(string_representation)
#                 chunk_id += 1
#                 chunks = np.zeros((len(atoms), chunk_size, 3))
#                 current_chunk_size = 0
#     logging.info(f"Transposed trajectory saved to {csv_out_path} in a geometry format")
#     return csv_out_path
#
#
# def transpose_trajectory_json_csv(u: Universe, out_dir_path: str | os.PathLike, modified_atom_names: dict[int, str], chunk_size: int = 1) -> None:
#     chunk_edge = chunk_size
#     chunk_id = 1
#     atom_dict = create_empty_atom_dict(modified_atom_names)
#     csv_out_path = os.path.join(out_dir_path, "chunks_json.csv")
#     atoms = u.select_atoms("all")
#     current_chunk_size = 0
#     with open(csv_out_path, 'w') as f:
#         for ts in tqdm(u.trajectory):
#             for atom_id, atom_name in enumerate(modified_atom_names):
#                 atom_dict[atom_name].append(list(atoms.positions[atom_id]))
#             current_chunk_size += 1
#             if ts.time == chunk_edge or ts.time == len(u.trajectory) - 1:
#                 json_string = f"{chunk_id};{chunk_size};{current_chunk_size};{TEMPERATURE};{HILLS};{{"
#                 first_atom = True
#                 for atom, coords in atom_dict.items():
#                     if not first_atom:
#                         json_string += ","
#                     json_string = f'{json_string}"{atom}": {coords}'
#                     first_atom = False
#                 json_string += "};1\n"
#                 f.write(json_string)
#                 chunk_edge += chunk_size
#                 chunk_id += 1
#                 atom_dict = create_empty_atom_dict(modified_atom_names)

#
# def create_empty_atom_dict(modified_atom_names):
#     atom_dict = {}
#     for atom_name in modified_atom_names:
#         atom_dict[atom_name] = []
#     return atom_dict

# def transpose_trajectory(u, modified_atom_names):
#     chunk_size = (len(u.trajectory) // 10)+1
#     chunk_edge = chunk_size
#     chunk_id = 1
#     atom_dict = create_empty_atom_dict(modified_atom_names)
#     atoms = u.select_atoms("all")
#     f = open("wf1_chunk.csv", 'w')
#     current_chunk_size = 0
#     for ts in tqdm(u.trajectory):
#         for atom_id, atom_name in enumerate(modified_atom_names):
#             atom_dict[atom_name].append(list(atoms.positions[atom_id]))
#         current_chunk_size += 1
#         if current_chunk_size == chunk_edge or ts.time == len(u.trajectory)-1:
#             logging.info(f"saving chunk with size {current_chunk_size}")
#             f.write(f"{chunk_id};{current_chunk_size};1;320;ParamHillsX;{{")
#             first_atom = True
#             for atom, coords in atom_dict.items():
#                 if not first_atom:
#                     f.write(",")
#                 f.write(f'"{atom}": ')
#                 f.write(f'{coords}')
#                 first_atom = False
#             f.write("};1\n")
#             current_chunk_size = 0
#             chunk_id += 1
#             atom_dict = create_empty_atom_dict(modified_atom_names)
#     f.close()
#
#
# def transpose_trajectory_stringbuilder(u, modified_atom_names):
#     chunk_size = len(u.trajectory) // 10
#     chunk_edge = chunk_size
#     chunk_id = 1
#     atoms = u.select_atoms("all")
#     f = open("wf1_chunk.csv", 'w')
#     total_dict_time = 0
#     arr = []
#     np.set_printoptions(threshold=np.inf)
#     np.set_printoptions(linewidth=np.inf)
#     for ts in u.trajectory:
#         dict_time_start = time.perf_counter()
#         arr.append(atoms.positions)
#         #for atom_id, atom_name in enumerate(modified_atom_names):
#             #atom_dict[atom_name].append(list(a[atom_id]))
#             #arr[atom_id].append(list(a[atom_id]))
#         dict_time_end = time.perf_counter()
#         total_dict_time += (dict_time_end - dict_time_start)
#         if ts.time == chunk_edge:
#             np_array =np.array(arr)
#             f.write(f"{chunk_id};{chunk_size};1;320;ParamHillsX;{{")
#             first_atom = True
#             for idx,atom in enumerate(modified_atom_names):
#                 coordinates = np.array2string(np_array[:,idx]).replace('\n', '').replace(' ', ', ')
#                 if not first_atom:
#                     f.write(",")
#                 f.write(f'"{atom}": {coordinates}')
#                 print(type(coordinates))
#                 first_atom = False
#             f.write("};1\n")
#             current_chunk_size = 0
#             chunk_id += 1
#             arr = []
#             if chunk_id == 3:
#                 break
#     print(f"dict time : {total_dict_time} seconds")
#     f.close()
#
#
# def transpose_trajectory_numpy(u, modified_atom_names):
#     chunk_size = len(u.trajectory) // 10
#     chunk_edge = chunk_size
#     chunk_id = 1
#     atom_dict = create_empty_atom_dict(modified_atom_names)
#     atoms = u.select_atoms("all")
#     f = open("wf1_chunk.csv", 'w')
#     for ts in u.trajectory:
#         for atom_id, atom_name in enumerate(modified_atom_names):
#             atom_dict[atom_name].append(list(atoms.positions[atom_id]))
#         if ts.time == chunk_edge :
#             json_string = f"{chunk_id};{chunk_size};1;320;ParamHillsX;{{"
#             first_atom = True
#             for atom, coords in atom_dict.items():
#                 if not first_atom:
#                     json_string += ","
#                 json_string = f'{json_string}"{atom}": {coords}'
#                 first_atom = False
#             json_string += "};1\n"
#             f.write(json_string)
#             chunk_edge += chunk_size
#             chunk_id += 1
#             if chunk_id == 3:
#                 break
#             else:
#                 atom_dict = create_empty_atom_dict(modified_atom_names)
#     f.close()
