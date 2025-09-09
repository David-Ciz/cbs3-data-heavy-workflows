import os

from py4lexis.lexis_irods import iRODS
from py4lexis.session import LexisSessionOffline

DEFAULT_ACCESS = 'project'
PROJECT = 'cbs3-ida'

DATASET_ID_FILE_NAME = "dataset_id.txt"

KEYRING_SERVICE_NAME = 'LEXIS_AUTH'
KEYRING_USERNAME = "offline_token"

dataset_folder = "hands_on_session/data/rerun_10is_traj/small/"

with open("offline_token.txt", "r") as text_file:
    access_token = text_file.read()

lexis_session = LexisSessionOffline(refresh_token=access_token)
irods = iRODS(session=lexis_session,
                    suppress_print=False)

response = irods.create_dataset(
    access=DEFAULT_ACCESS, project=PROJECT, title="small_simulation"
)

dataset_id = response["dataset_id"]


for filename in os.listdir(dataset_folder):
    filepath = os.path.join(dataset_folder, filename)
    if os.path.isfile(filepath):
        irods.put_data_object_to_dataset(
            local_filepath=filepath,
            access=DEFAULT_ACCESS, project=PROJECT,
            dataset_id=dataset_id,
        )