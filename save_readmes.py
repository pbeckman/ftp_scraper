import os
from petrel_metadata_collector import get_globus_client, download_file

PETREL_ID = os.environ["PETREL_ID"]
LOCAL_ID = os.environ["LOCAL_ID"]
TRANSFER_TOKEN = os.environ["TRANSFER_TOKEN"]


def save_readmes(tc, endpoint_id, local_path, files, start_file_number):
    for full_file_name in files[start_file_number:]:
        globus_path, file_name = full_file_name.strip().rsplit("/", 1)
        if "readme" in file_name.lower():
            download_file(tc, endpoint_id, globus_path, file_name, local_path)

tc = get_globus_client()

with open("pub8_list.txt", "r") as file_list:
    save_readmes(tc, PETREL_ID, "/home/paul/", file_list.readlines(), 0)
