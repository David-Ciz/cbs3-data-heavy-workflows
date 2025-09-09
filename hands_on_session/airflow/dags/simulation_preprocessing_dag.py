import base64
import pathlib
import shutil
import itertools

from airflow.models import Variable
from airflow.models.param import Param
from py4lexis.core.lexis_irods import iRODS
from py4lexis.core.session import LexisSessionOffline
from py4lexis.core.ddi.datasets import Datasets

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from typing import List, Tuple
from MDAnalysis import Universe
from tempfile import NamedTemporaryFile
import os
from tempfile import mkdtemp
import re
from pathlib import Path

from ida4sims.db import Molecule
from ida4sims.db import Simulation
from ida4sims.db import SimulationPath
from ida4sims.transpose import transpose_trajectory_hdf5
from ida4sims.utils import update_path_to_file_in_directory
from ida4sims.update_preprocessing import organize_files
from ida4sims.update_preprocessing import check_and_prepare_trajectories
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

def drop_schema_on_failure(context):
    print("CALLBACK: drop_schema_on_failure called")
    ti = context['ti']
    try:
        schema_name = ti.xcom_pull(task_ids='generate_schema', key='return_value')[0][0]
        hook = PostgresHook(postgres_conn_id='postgres_ida4sims_staging')
        hook.run(f"SELECT * FROM _master.drop_schema('{schema_name}');")
        print(f"Dropped schema {schema_name} due to DAG failure.")
    except Exception as e:
        print(f"Failed to drop schema on failure: {e}")

dag = DAG(
    'simulation_processing',
    default_args=default_args,
    description='Process simulation data, insert into database and transfer hdf5 file to server',
    schedule_interval=None,
    params={
        'dataset_id': Variable.get('dataset_id', ''),
        'username': Variable.get('username',''),
        'user_id': Variable.get('user_id',''),
        'do_partial_processing': Param(type='boolean', default=False, description='Do partial processing, only the first 1000 timesteps will be processed'),
        'do_cleanup': Param(type='boolean', default=True, description='Cleanup after run'),
    },
    on_failure_callback=drop_schema_on_failure,
)





class DynamicSchemaPostgresHook(PostgresHook):
    def __init__(self, *args, **kwargs):
        self.schema = kwargs.pop('schema', None)
        super().__init__(*args, **kwargs)

    def get_conn(self):
        conn = super().get_conn()
        if self.schema:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {self.schema}")
        return conn



# def prepare_atom_data(**kwargs):
#     ti = kwargs['ti']
#     simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')
#     universe = Universe(path_to_topology, path_to_trajectory)
#     atoms_data = atom_processing(simulation_id, universe)
#     kwargs['ti'].xcom_push(key='atoms_data', value=atoms_data)

###############################################################################################
####    FUNCTIONS 
###############################################################################################
def get_dataset_metadata(datasets: Datasets, dataset_id: str) -> dict:
    """
    Get the metadata of the dataset with the given ID.
    """
    try:
        filelist = datasets.get_all_datasets(dataset_id=dataset_id)
        return filelist[0]['additionalMetadata']

    except Exception as e:
        print(f"Error checking dataset contents: {e}")
        return False
    
def get_restraint_file(restaint_file_path: str = "restraint_file_path.txt"):
    if pathlib.Path("restraint_file_path.txt").is_file():
        with open("restraint_file_path.txt", "r") as text_file:
            path_to_restraint_file = text_file.read()
        if pathlib.Path(path_to_restraint_file).is_file():
            with open(path_to_restraint_file, 'rb') as file:
                return file.read()
        else:
            return None
    else:
        return None

def download_dataset_from_irods(**kwargs):
    ti = kwargs['ti']
    refresh_token = Variable.get("lexis_refresh_token")
    lexis_session = LexisSessionOffline(refresh_token=refresh_token)
    irods = iRODS(session=lexis_session,
                  suppress_print=False)

    dataset_id = kwargs['dag_run'].conf.get('dataset_id') or kwargs['params']['dataset_id']
    # Create temporary directory and store its path
    downloaded_data_directory_path = os.path.join("processing_data", "downloaded_data","simulations")
    os.makedirs(downloaded_data_directory_path, exist_ok=True)
    print('downloaded_data_directory_path: ' + str(downloaded_data_directory_path))

    ## Push path if download will fail
    ti.xcom_push(key='downloaded_data_directory_path', value=str(downloaded_data_directory_path))

    # Check if the dataset already exists in the directory
    if os.path.exists(os.path.join(downloaded_data_directory_path, dataset_id)):
        print("Dataset already exists, skipping download.")
    else:
        irods.download_dataset_as_directory(access="project", project="exa4mind_wp4", dataset_id=dataset_id, local_directorypath=downloaded_data_directory_path)


    dataset_folder = pathlib.Path(downloaded_data_directory_path, dataset_id)

    path_to_sim_out_data_dir, path_to_trajectory_dir, path_to_topologies_dir = organize_files(dataset_folder, debug=False)

    datasets = Datasets(session=lexis_session, suppress_print=False)
    dataset_metadata = get_dataset_metadata(datasets, dataset_id)

    ti.xcom_push(key='path_to_topologies_dir', value=str(path_to_topologies_dir))
    ti.xcom_push(key='path_to_trajectory_dir', value=str(path_to_trajectory_dir))
    ti.xcom_push(key='path_to_sim_out_data_dir', value=str(path_to_sim_out_data_dir))
    ti.xcom_push(key='dataset_metadata', value=dataset_metadata)

def check_and_prepare_data(**kwargs):
    ti = kwargs['ti']
    path_to_topologies_dir = ti.xcom_pull(task_ids='download_dataset', key='path_to_topologies_dir')
    path_to_trajectory_dir = ti.xcom_pull(task_ids='download_dataset', key='path_to_trajectory_dir')
    path_to_sim_out_data_dir = ti.xcom_pull(task_ids='download_dataset', key='path_to_sim_out_data_dir')
    trajectory_files, topology_files, out_files, sim_flag, ioutformat, sorted_plumed_files = check_and_prepare_trajectories(path_to_sim_out_data_dir, path_to_trajectory_dir, path_to_topologies_dir, debug=True)

    # Explicitly push to XCom
    ti.xcom_push(key='topology_files', value=str(topology_files))
    ti.xcom_push(key='trajectory_files', value=str(trajectory_files))
    ti.xcom_push(key='out_files', value=str(out_files))
    ti.xcom_push(key='sim_flag', value=str(sim_flag))
    ti.xcom_push(key='ioutformat', value=str(ioutformat))
    ti.xcom_push(key='sorted_plumed_files', value=str(sorted_plumed_files))


""" def prepare_molecule_data(**kwargs):
    ti = kwargs['ti']

    path_to_topology = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='topology_files'))
    path_to_trajectory = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='trajectory_files'))
    ioutformat = ti.xcom_pull(task_ids='check_and_prepare_data', key='ioutformat')
    trajectory_files= path_to_trajectory[0]
    topology_file = path_to_topology[0]
    print('topology_file: '+str(topology_file))
    print('trajectory_files: '+str(trajectory_files))
    print('ioformat: '+str(ioutformat))

    universe = Universe(topology_file, trajectory_files, topology_format='TOP', format=ioutformat)

    metadata = {}
    molecule = Molecule.from_molecule_data(universe, topology_file, metadata)
    print("Molecule created successfully")

    molecule_data = molecule.__dict__
    # Convert type list to string representation
    molecule_data['type'] = '{' + ','.join(molecule_data['type']) + '}'

    # Explicitly push to XCom
    ti.xcom_push(key='molecule_data', value=molecule_data)

    return molecule_data
 """

def prepare_simulation_data(**kwargs):
    ti = kwargs['ti']
    dataset_id = kwargs['dag_run'].conf.get('dataset_id') or kwargs['params']['dataset_id']
    path_to_topology = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='topology_files'))
    path_to_trajectory = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='trajectory_files'))
    path_to_sim_out_data = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='out_files'))
    ioutformat = ti.xcom_pull(task_ids='check_and_prepare_data', key='ioutformat')    
    sim_flag = ti.xcom_pull(task_ids='check_and_prepare_data', key='sim_flag')
    metadata = ti.xcom_pull(task_ids='download_dataset_from_irods', key='dataset_metadata')
    trajectory_files= path_to_trajectory[0]
    topology_file = path_to_topology[0]
    sim_out_files = path_to_sim_out_data
    out_file= path_to_sim_out_data[0][0]
    upload_user_id = ti.xcom_pull(task_ids='get_user_id')[0][0]

    #### TODO: change this later when we will have detection molecule done
    molecule_id = 1 #ti.xcom_pull(task_ids='insert_molecule', key='return_value')[0][0]

    universe = Universe(topology_file, trajectory_files, topology_format='TOP', format=ioutformat)
    
    if pathlib.Path(out_file).is_file():
        with open(out_file, 'r') as f:
            sim_out_file = f.read()
    else:
        sim_out_file = ""
    
    simulation = Simulation.from_simulation_data(universe, dataset_id, topology_file, sim_out_files, sim_out_file, molecule_id, upload_user_id,sim_flag, metadata)
    simulation_data = simulation.__dict__

    # Handle non-JSON serializable types and None values
    for key, value in simulation_data.items():
        if value is None:
            simulation_data[key] = 'None'
        elif isinstance(value, datetime):
            simulation_data[key] = value.isoformat()
        elif isinstance(value, bytes):
            simulation_data[key] = base64.b64encode(value).decode('utf-8')
        elif isinstance(value, (list, tuple)):
            simulation_data[key] = [str(item) if item is not None else 'None' for item in value]
    print(f"simulation_data: {simulation_data}")
    return simulation_data


def prepare_atoms_data_for_sql(**kwargs):
    ti = kwargs['ti']
    path_to_topology = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='topology_files'))
    path_to_trajectory = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='trajectory_files'))
    
    ioutformat = ti.xcom_pull(task_ids='check_and_prepare_data', key='ioutformat')
    trajectory_files= path_to_trajectory[0]
    topology_file = path_to_topology[0]
    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]
    schema_name = ti.xcom_pull(task_ids='generate_schema', key='return_value')[0][0]

    universe = Universe(topology_file, trajectory_files, format=ioutformat)

    live_db_hook = PostgresHook(postgres_conn_id='postgres_ida4sims_live')
    atom_count = len(universe.atoms)
    start_id = live_db_hook.get_first(f"SELECT nextval('atom_id_seq') as id")[0]
    end_id = start_id + atom_count - 1
    live_db_hook.run(f"SELECT setval('atom_id_seq', {end_id})")
    # Generate SQL file
    sql_filename = "/opt/airflow/dags/atoms_schema.sql"
    #sql_filename = f"atoms_insert_{kwargs['dag_run'].run_id}.sql"

    def escape_sql_string(s):
        return s.replace("'", "''")

    # Create a temporary file that persists until explicitly deleted
    temp_dir = '/tmp/airflow_sql'  # or another appropriate directory in your container
    os.makedirs(temp_dir, exist_ok=True)

    temp_file = NamedTemporaryFile(
        mode='w',
        suffix='.sql',
        dir=temp_dir,
        delete=False  # Important: don't delete immediately
    )

    try:
        with temp_file as sql_file:
            sql_file.write(
                f"INSERT INTO {schema_name}.atom (id, order_id, simulation_id, atom_name, res_name, conc_name) VALUES\n")

            values = []
            for order_id, atom in enumerate(universe.atoms):
                res_name = f":{atom.resid}@"
                conc_name = f"{res_name}{atom.name}"
                values.append(
                    f"({start_id + order_id}, {order_id}, {simulation_id}, '{escape_sql_string(atom.name)}', '{escape_sql_string(res_name)}', '{escape_sql_string(conc_name)}')")

            if values:
                sql_file.write(',\n'.join(values) + ';\n')

        return temp_file.name  # This will automatically be stored in XCom

    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise e
    #absolute_sql_filename_path = pathlib.Path(sql_filename).absolute()
    #print(absolute_sql_filename_path)
    #return sql_filename


def prepare_atoms_data(**kwargs) -> List[Tuple]:
    ti = kwargs['ti']
    """
    Atoms belong to certain residues. Residue information for the atoms is important for analysis definition.
    To save time later, we already concatenate the information together, but also keep the original for other possible uses.
    """
    path_to_topology = ti.xcom_pull(task_ids='download_dataset', key='path_to_topology')
    path_to_trajectory = ti.xcom_pull(task_ids='download_dataset', key='path_to_trajectory')

    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]
    universe = Universe(path_to_topology, path_to_trajectory, format='TRJ')

    atom_data: List[Tuple] = []
    for order_id, atom in enumerate(universe.atoms):
        res_name = f":{atom.resid}@"
        conc_name = f"{res_name}{atom.name}"
        atom_data.append((
            order_id,
            simulation_id,
            atom.name,
            res_name,
            conc_name
        ))

    return atom_data





#
# def get_parameters(**context):
#     ti = context['ti']
#     simulation_data = ti.xcom_pull(task_ids='prepare_simulation_data')
#     simulation_data['restraint_file'] = get_restraint_file()
#     return simulation_data


def create_and_prepare_hdf5(**kwargs):
    ti = kwargs['ti']
    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]
    path_to_topology = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='topology_files'))
    path_to_trajectory = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='trajectory_files'))
    sim_flag = ti.xcom_pull(task_ids='check_and_prepare_data', key='sim_flag')
    do_partial_processing = kwargs['dag_run'].conf.get('do_partial_processing') or kwargs['params']['do_partial_processing']
    ioutformat = ti.xcom_pull(task_ids='check_and_prepare_data', key='ioutformat')
    dataset_id = kwargs['dag_run'].conf.get('dataset_id') or kwargs['params']['dataset_id']

    # Prepare output directory
    hdf5_data_directory_path = os.path.join("processing_data", "hdf5_data",dataset_id)
    os.makedirs(hdf5_data_directory_path, exist_ok=True)
    print('hdf5_data_directory_path: ' + str(hdf5_data_directory_path))

    out_dir_path = hdf5_data_directory_path
    ti.xcom_push(key='hdf5_out_dir_path', value=str(out_dir_path))
    # Initialize list to store HDF5 file paths
    hdf5_local_file_paths = []
    hdf5_name = None
    replica_id = ''

    for i, traj_group in enumerate(path_to_trajectory):
        topo = path_to_topology[i]
            
        # Add suffix depend on sim_flag
        if sim_flag not in ["standart_simulation", "meta_dynamic_simulation (WT-MetaD)"]:
            # Extract replica_id from name of trajectory
            match = re.search(r'_rep\d+', traj_group[0])
            if match:
                replica_id = match.group(0)
                hdf5_name = f"{simulation_id}{replica_id}_trajectory.h5"
        
            print(topo, traj_group, out_dir_path, replica_id)
        else:
                
            hdf5_name = f"{simulation_id}_trajectory.h5"
            print(topo, traj_group, out_dir_path)
        u = Universe(topo, traj_group, format=ioutformat)                    
        hdf5_local_file_path = transpose_trajectory_hdf5(u, out_dir_path, hdf5_name, do_partial_processing)
        hdf5_local_file_paths.append(hdf5_local_file_path)
    print('hdf5_files:' + str(hdf5_local_file_paths))


    #ti.xcom_push(key='hdf5_file_name', value=hdf5_name)
    ti.xcom_push(key='hdf5_local_file_paths', value=str(hdf5_local_file_paths))

def transfer_hdf5_to_irods(**kwargs):
    ti = kwargs['ti']
    dataset_ids = []
    refresh_token = Variable.get("lexis_refresh_token")
    lexis_session = LexisSessionOffline(refresh_token=refresh_token)
    irods = iRODS(session=lexis_session,
                  suppress_print=False)

    hdf5_local_file_paths = eval(ti.xcom_pull(task_ids='create_and_prepare_hdf5', key='hdf5_local_file_paths'))
    for hdf5_local_file_path in hdf5_local_file_paths:
        create_dataset_response = irods.create_dataset(access="project",
                                                       project="exa4mind_wp4",
                                                       title=f"{Path(hdf5_local_file_path).stem}",)
        dataset_ids.append(create_dataset_response['dataset_id'])
        irods.put_data_object_to_dataset(local_filepath=hdf5_local_file_path,
                                         dataset_id=create_dataset_response['dataset_id'],
                                         access="project",
                                         project="exa4mind_wp4",
                                         dataset_filepath="./")
    # TODO: change this hardcoded storage from LEXIS iRODS to allow other storages

    ti.xcom_push(key='storage_type', value='iRODS')
    ti.xcom_push(key='irods_unique_ids', value=str(dataset_ids))
    ti.xcom_push(key='irods_project_short_name', value='exa4mind_wp4')
    ti.xcom_push(key='irods_zone', value='IT4ILexisV2')

def prepare_hdf5_for_sql(**kwargs):
    ti = kwargs['ti']

    schema_name = ti.xcom_pull(task_ids='generate_schema', key='return_value')[0][0]
    dataset_ids =eval(ti.xcom_pull(task_ids='transfer_hdf5_to_irods', key='irods_unique_ids'))
    storage_type = ti.xcom_pull(task_ids='transfer_hdf5_to_irods', key='storage_type')
    irods_project_short_name = ti.xcom_pull(task_ids='transfer_hdf5_to_irods', key='irods_project_short_name')
    irods_zone = ti.xcom_pull(task_ids='transfer_hdf5_to_irods', key='irods_zone')
    hdf5_local_file_paths = eval(ti.xcom_pull(task_ids='create_and_prepare_hdf5', key='hdf5_local_file_paths'))
    number_of_dataset_ids = len(dataset_ids)
    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]

    live_db_hook = PostgresHook(postgres_conn_id='postgres_ida4sims_live')

    start_id = live_db_hook.get_first(f"SELECT nextval('hdf5_files_id_seq') as id")[0]
    end_id = start_id + number_of_dataset_ids - 1
    live_db_hook.run(f"SELECT setval('hdf5_files_id_seq', {end_id})")


    # Create a temporary file that persists until explicitly deleted
    temp_dir = '/tmp/airflow_sql'  # or another appropriate directory in your container
    os.makedirs(temp_dir, exist_ok=True)

    temp_file = NamedTemporaryFile(
        mode='w',
        suffix='.sql',
        dir=temp_dir,
        delete=False  # Important: don't delete immediately
    )

    try:
        with temp_file as sql_file:
            sql_file.write(
                f"INSERT INTO {schema_name}.hdf5_files (id, simulation_id, storage_type, file_name, irods_unique_id, irods_project_short_name, irods_zone) VALUES\n")
            values = []
            for order_id,(hdf5_local_file_path, irods_unique_id) in enumerate(zip(hdf5_local_file_paths,dataset_ids)):
                file_name = Path(hdf5_local_file_path).name
                values.append(
                    f"({start_id + order_id}, {simulation_id}, '{storage_type}', '{file_name}', '{irods_unique_id}', '{irods_project_short_name}', '{irods_zone}')")

            if values:
                sql_file.write(',\n'.join(values) + ';\n')

        ti.xcom_push(key='temp_file_name', value=temp_file.name)  # This will automatically be stored in XCom
        ti.xcom_push(key='start_id', value=start_id)

    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise e
    
    
    #absolute_sql_filename_path = pathlib.Path(sql_filename).absolute()
    #print(absolute_sql_filename_path)
    #return sql_filename



def prepare_path_data_for_sql(**kwargs):
    ti = kwargs['ti']
    hdf5_start_id = ti.xcom_pull(task_ids='prepare_hdf5_for_sql', key='start_id')
    ioutformat = ti.xcom_pull(task_ids='check_and_prepare_data', key='ioutformat')
    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]
    path_to_topology = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='topology_files'))
    path_to_trajectory = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='trajectory_files'))
    path_to_plumed_file = eval(ti.xcom_pull(task_ids='check_and_prepare_data', key='sorted_plumed_files'))
    schema_name = ti.xcom_pull(task_ids='generate_schema', key='return_value')[0][0]
    following_parameter = ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_parameter_type']
    ensemble_ladder = ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_ladder']

    number_of_paths = len(path_to_topology)
    simulation_id = ti.xcom_pull(task_ids='insert_simulation', key='return_value')[0][0]

    live_db_hook = PostgresHook(postgres_conn_id='postgres_ida4sims_live')

    start_id = live_db_hook.get_first(f"SELECT nextval('path_id_seq') as id;")[0]
    end_id = start_id + number_of_paths - 1
    live_db_hook.run(f"SELECT setval('path_id_seq', {end_id})")
    
    # Create an iterator for ensemble ladder values
    if ensemble_ladder and isinstance(ensemble_ladder, list) and len(ensemble_ladder) > 0:
        ladder_iter = itertools.cycle(ensemble_ladder)
    else:
        ladder_iter = itertools.cycle([None])  # nebo ['NULL']


    # Create a temporary file that persists until explicitly deleted
    temp_dir = '/tmp/airflow_sql'  # or another appropriate directory in your container
    os.makedirs(temp_dir, exist_ok=True)

    temp_file = NamedTemporaryFile(
        mode='w',
        suffix='.sql',
        dir=temp_dir,
        delete=False  # Important: don't delete immediately
    )

    try:
        with temp_file as sql_file:
            sql_file.write(
                f"INSERT INTO {schema_name}.path (id, run_name, type, box_initial, box_change, box_final, following_parameter, following_value, simulation_id, plumed, ff_id, hdf5_file_id, topo_file_name, topo_file) VALUES\n")
            values = []
            for order_id,(topology_file, trajectory_files,plumed_file) in enumerate(zip(path_to_topology,path_to_trajectory, path_to_plumed_file)):
                universe =  Universe(topology_file, trajectory_files, format=ioutformat)
                hdf5_file_id = hdf5_start_id + order_id
                # Pass topology_file as argument for extraction
                path = SimulationPath(universe, simulation_id, hdf5_file_id, topology_file_path=topology_file,plumed_file_path=plumed_file)
                box_initial = '{' + ','.join(map(str, map(float, path.box_initial))) + '}'
                box_final = '{' + ','.join(map(str, map(float, path.box_final))) + '}'
                print(path)
                ff_id = 'NULL' if path.ff_id is None else path.ff_id

                # Prepare topo_file_name and topo_file for SQL (bytea as hex string)
                topo_file_name = f"'{path.topo_file_name}'" if path.topo_file_name else 'NULL'
                if path.topo_file is not None:
                    import psycopg2
                    topo_file_hex = psycopg2.Binary(path.topo_file).getquoted().decode()
                else:
                    topo_file_hex = 'NULL'

                if path.plumed is not None:
                    import psycopg2
                    plumed_hex = psycopg2.Binary(path.plumed).getquoted().decode()
                else:
                    plumed_hex = 'NULL'

                # Get next value from the ensemble ladder
                following_value = next(ladder_iter)
                if following_value is None:
                    following_value = 'NULL'
                else:
                    following_value = f"'{following_value}'"

                values.append(
                    f"({start_id + order_id}, '{path.run_name}', '{path.type}', '{box_initial}', {path.box_change}, '{box_final}', '{following_parameter}', {following_value},{path.simulation_id}, {plumed_hex}, {ff_id}, {path.hdf5_file_id}, {topo_file_name}, {topo_file_hex})")

                if len(values) == 1000:
                    sql_file.write(',\n'.join(values) + ';\n')
                    values = []

            if values:
                sql_file.write(',\n'.join(values) + ';\n')

        return temp_file.name  # This will automatically be stored in XCom

    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise e

    
def execute_sql_atoms_file(**kwargs):
    ti = kwargs['ti']
    sql_file_path = ti.xcom_pull(task_ids='prepare_atoms_sql')

    with open(sql_file_path, 'r') as file:
        sql = file.read()

    hook = PostgresHook(postgres_conn_id='postgres_ida4sims_staging')
    hook.run(sql)

def execute_sql_hdf5_file(**kwargs):
    ti = kwargs['ti']
    sql_file_path = ti.xcom_pull(task_ids='prepare_hdf5_for_sql', key='temp_file_name')

    with open(sql_file_path, 'r') as file:
        sql = file.read()

    hook = PostgresHook(postgres_conn_id='postgres_ida4sims_staging')
    hook.run(sql)

def execute_sql_path_file(**kwargs):
    ti = kwargs['ti']
    sql_file_path = ti.xcom_pull(task_ids='prepare_path_for_sql')

    with open(sql_file_path, 'r') as file:
        sql = file.read()

    hook = PostgresHook(postgres_conn_id='postgres_ida4sims_staging')
    hook.run(sql)

def clean_successful(**kwargs):
    """
    Delete downloaded data, created HDF5 files, and all temporary SQL files if everything went well.
    """
    ti = kwargs['ti']
    do_cleanup = kwargs['dag_run'].conf.get('do_cleanup') or kwargs['params']['do_cleanup']
    if do_cleanup:
        # Delete dataset folder
        downloaded_data_directory_path = ti.xcom_pull(task_ids='download_dataset', key='downloaded_data_directory_path')
        dataset_id = kwargs['dag_run'].conf.get('dataset_id') or kwargs['params']['dataset_id']
        dataset_folder = pathlib.Path(downloaded_data_directory_path, dataset_id)
        if dataset_folder.exists() and dataset_folder.is_dir():
            shutil.rmtree(dataset_folder)
            print(f"Deleted dataset folder: {dataset_folder}")

        # Delete HDF5 directory
        hdf5_out_dir_path = ti.xcom_pull(task_ids='create_and_prepare_hdf5', key='hdf5_out_dir_path')
        if hdf5_out_dir_path and os.path.exists(hdf5_out_dir_path):
            try:
                shutil.rmtree(hdf5_out_dir_path)
                print(f"Deleted HDF5 directory: {hdf5_out_dir_path}")
            except Exception as e:
                print(f"Error deleting HDF5 directory: {e}")

        # Delete all temporary SQL files
        temp_files = [
            ti.xcom_pull(task_ids='prepare_atoms_sql', key='return_value'),
            ti.xcom_pull(task_ids='prepare_hdf5_for_sql', key='temp_file_name'),
            ti.xcom_pull(task_ids='prepare_path_for_sql', key='return_value')
        ]
        for f in temp_files:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                    print(f"Deleted temp SQL file: {f}")
                except Exception as e:
                    print(f"Error deleting temp SQL file {f}: {e}")

def get_user_id_safe(**kwargs):
    ti = kwargs['ti']
    user_id_param = kwargs['dag_run'].conf.get('user_id') or kwargs['params'].get('user_id')

    ## If user ID is not provided, we return a default value (James007)
    if not user_id_param:
        print("user_id not provided, using default user James007.")
        ti.xcom_push(key='return_value', value=[[1]])
        return [[1]]

    hook = PostgresHook(postgres_conn_id='postgres_ida4sims_staging')
    result = hook.get_records(
        "SELECT id FROM _master.user WHERE user_id = %s;", parameters=[user_id_param]
    )
    if not result or not result[0]:
        print(f"User with user_id '{user_id_param}' not found, using default user James007.")
        ti.xcom_push(key='return_value', value=[[1]])
        return [[1]]
    ti.xcom_push(key='return_value', value=result)
    return result

with dag:

    # Dynamic schema generation to seperate active database from uploading process for safer incorporation.

    generate_schema = SQLExecuteQueryOperator(
        task_id='generate_schema',
        conn_id='postgres_ida4sims_staging',
        sql="""
        SELECT _master.generate_schema(%s) AS schema_name;
        """,
        parameters=["{{ params.username }}"],
        do_xcom_push=True,
        trigger_rule='all_success'
    )

    get_user_id = PythonOperator(
        task_id='get_user_id',
        python_callable=get_user_id_safe,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    # Download of dataset and processing

    download_dataset = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset_from_irods,
        provide_context=True,
        trigger_rule='all_success'
    )

    check_and_prepare_data_task = PythonOperator(
        task_id='check_and_prepare_data',
        python_callable=check_and_prepare_data,
        provide_context=True,
        trigger_rule='all_success'
    )

#    prepare_molecule = PythonOperator(
#        task_id='prepare_molecule_data',
#        python_callable=prepare_molecule_data,
#    )

#    reserve_molecule_id = SQLExecuteQueryOperator(
#        task_id='reserve_molecule_id',
#        conn_id='postgres_ida4sims_live',
#        sql="SELECT nextval('molecule_id_seq') as id;",
#    )
    #TODO: change the returning id and have everything that uses it actually use the id from reserve.
#    insert_molecule = SQLExecuteQueryOperator(
#        task_id='insert_molecule',
#        conn_id='postgres_ida4sims_staging',
#        sql="""
#                INSERT INTO {{ task_instance.xcom_pull(task_ids='generate_schema', key='return_value')[0][0] }}.molecule (id, sequence, type, organism, organism_description, upload_timestamp, upload_user_id)
#                VALUES (
#                    {{ task_instance.xcom_pull(task_ids='reserve_molecule_id', key='return_value')[0][0] }},
#                    '{{ ti.xcom_pull(task_ids='prepare_molecule_data')['sequence'] }}',
#                    '{{ ti.xcom_pull(task_ids='prepare_molecule_data')['type'] }}',
#                    '{{ ti.xcom_pull(task_ids='prepare_molecule_data')['organism'] }}',
#                    '{{ ti.xcom_pull(task_ids='prepare_molecule_data')['organism_description'] }}',
#                    '{{ ti.xcom_pull(task_ids='prepare_molecule_data')['upload_timestamp'] }}',
#                    {{ task_instance.xcom_pull(task_ids='get_user_id', key='return_value')[0][0] }}
#                )
#                RETURNING id;
#                """
#    )
    prepare_simulation = PythonOperator(
        task_id='prepare_simulation_data',
        python_callable=prepare_simulation_data,
        trigger_rule='all_success'
    )

    reserve_simulation_id = SQLExecuteQueryOperator(
        task_id='reserve_simulation_id',
        conn_id='postgres_ida4sims_live',
        sql="SELECT nextval('simulation_id_seq') as id;",
        trigger_rule='all_success'
    )

    insert_simulation = SQLExecuteQueryOperator(
        task_id='insert_simulation',
        conn_id='postgres_ida4sims_staging',
        sql="""
        INSERT INTO {{ task_instance.xcom_pull(task_ids='generate_schema', key='return_value')[0][0] }}.simulation (
            id, lexis_dataset_id,doi, author_name, reference_article_doi, upload_user_id, software_name, software_settings, software_version,
            creation_timestamp, upload_timestamp, simulation_description, sim_name, solvent, ion_type,
            ionic_concentration, num_snapshots, stripping_mask, ensemble_parameter_type, time_step,
            total_sim_time, thermostat, barostat, ensemble_ladder, restraint_file, molecule_id,
            ensemble, temperature, pressure
        ) VALUES (
            {{ task_instance.xcom_pull(task_ids='reserve_simulation_id', key='return_value')[0][0] }},            
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['lexis_dataset_id'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['lexis_dataset_id'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['doi'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['doi'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['author_name'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['author_name'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['reference_article_doi'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['reference_article_doi'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['upload_user_id'] if ti.xcom_pull(task_ids='prepare_simulation_data')['upload_user_id'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['software_name'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['software_name'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['software_settings'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['software_settings'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['software_version'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['software_version'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['creation_timestamp'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['creation_timestamp'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['upload_timestamp'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['upload_timestamp'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['simulation_description'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['simulation_description'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['sim_name'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['sim_name'] != 'None' else 'NULL' }},
            {{ "'{" + ",".join(ti.xcom_pull(task_ids='prepare_simulation_data')['solvent']) + "}'" if ti.xcom_pull(task_ids='prepare_simulation_data')['solvent'] != 'None' else 'NULL' }},
            {{ "'{" + ",".join(ti.xcom_pull(task_ids='prepare_simulation_data')['ion_type']) + "}'" if ti.xcom_pull(task_ids='prepare_simulation_data')['ion_type'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['ionic_concentration'] if ti.xcom_pull(task_ids='prepare_simulation_data')['ionic_concentration'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['num_snapshots'] if ti.xcom_pull(task_ids='prepare_simulation_data')['num_snapshots'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['stripping_mask'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['stripping_mask'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_parameter_type'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_parameter_type'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['time_step'] if ti.xcom_pull(task_ids='prepare_simulation_data')['time_step'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['total_sim_time'] if ti.xcom_pull(task_ids='prepare_simulation_data')['total_sim_time'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['thermostat'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['thermostat'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['barostat'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['barostat'] != 'None' else 'NULL' }},
            {{ "'{" + ",".join(ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_ladder']) + "}'" if ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble_ladder'] != 'None' else 'NULL' }},
            %s,
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['molecule_id'] if ti.xcom_pull(task_ids='prepare_simulation_data')['molecule_id'] != 'None' else 'NULL' }},
            {{ "'" + ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble'] + "'" if ti.xcom_pull(task_ids='prepare_simulation_data')['ensemble'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['temperature'] if ti.xcom_pull(task_ids='prepare_simulation_data')['temperature'] != 'None' else 'NULL' }},
            {{ ti.xcom_pull(task_ids='prepare_simulation_data')['pressure'] if ti.xcom_pull(task_ids='prepare_simulation_data')['pressure'] != 'None' else 'NULL' }}
        ) RETURNING id;
        """,
        parameters=[get_restraint_file()],
        trigger_rule='all_success'
    )

    prepare_atoms_sql = PythonOperator(
        task_id='prepare_atoms_sql',
        python_callable=prepare_atoms_data_for_sql,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    insert_atoms = PythonOperator(
        task_id='insert_atoms_data',
        python_callable=execute_sql_atoms_file,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    prepare_hdf5 = PythonOperator(
        task_id='create_and_prepare_hdf5',
        python_callable=create_and_prepare_hdf5,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    transfer_hdf5_to_irods_task = PythonOperator(
        task_id='transfer_hdf5_to_irods',
        python_callable=transfer_hdf5_to_irods,
        trigger_rule='all_success'
    )

    prepare_hdf5_for_sql_task = PythonOperator(
        task_id='prepare_hdf5_for_sql',
        python_callable=prepare_hdf5_for_sql,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    insert_hdf5_task =  PythonOperator(
        task_id='insert_hdf5_into_db',
        python_callable=execute_sql_hdf5_file,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )
    # transfer_hdf5_task = SFTPOperator(
    #     task_id='transfer_hdf5_to_server',
    #     ssh_conn_id='sftp_adams4sims',  # Use your SFTP connection ID here
    #     local_filepath="{{ task_instance.xcom_pull(task_ids='create_and_prepare_hdf5')['hdf5_local_file_path'] }}",
    #     remote_filepath="{{ task_instance.xcom_pull(task_ids='create_and_prepare_hdf5')['hdf5_data']['folder'] }}/{{ task_instance.xcom_pull(task_ids='create_and_prepare_hdf5')['hdf5_data']['rel_path'] }}",
    #     operation='put',
    #     create_intermediate_dirs=True,
    #     dag=dag,
    # )

    prepare_path_for_sql = PythonOperator(
        task_id="prepare_path_for_sql",
        python_callable=prepare_path_data_for_sql,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    insert_path_task =  PythonOperator(
        task_id='insert_path_into_db',
        python_callable=execute_sql_path_file,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success'
    )

    clean_successful_task = PythonOperator(
    task_id='clean_successful_task',
    python_callable=clean_successful,
    provide_context=True,
    trigger_rule='all_success',  # Spustí se jen pokud všechny předchozí tasky uspěly
    dag=dag
)

    clean_failed_task = PythonOperator(
    task_id='clean_failed_task',
    python_callable=clean_successful,  # vytvoř funkci pro úklid při selhání
    provide_context=True,
    trigger_rule='one_failed',  # Spustí se pokud alespoň jeden předchozí task selže
    dag=dag
)
    
    download_dataset >> check_and_prepare_data_task >> generate_schema >> get_user_id

    #generate_schema >> prepare_molecule >> reserve_molecule_id >> insert_molecule
    #insert_molecule >> prepare_simulation >> reserve_simulation_id >> insert_simulation
    
    ##delete this after fix molecule
    get_user_id >> prepare_simulation >> reserve_simulation_id >> insert_simulation

    insert_simulation >> prepare_atoms_sql >> insert_atoms

    insert_simulation >> prepare_hdf5 >> transfer_hdf5_to_irods_task >> prepare_hdf5_for_sql_task >> insert_hdf5_task

    insert_hdf5_task >> prepare_path_for_sql >>  insert_path_task >> [clean_successful_task, clean_failed_task]