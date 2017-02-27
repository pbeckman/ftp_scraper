import os
import globus_sdk
from petrel_metadata_collector import get_globus_client

PETREL_ID = os.environ["PETREL_ID"]
LOCAL_ID = os.environ["LOCAL_ID"]
TRANSFER_TOKEN = os.environ["TRANSFER_TOKEN"]


def save_readmes(tc, endpoint_id, local_path, files, start_file_number):
    for i in range(start_file_number, len(files)):
        full_file_name = files[i]
        globus_path, file_name = full_file_name.strip().rsplit("/", 1)
        globus_path += "/"
        if "readme" in file_name.lower():
            print("downloading file {}".format(globus_path + file_name))
            tdata = globus_sdk.TransferData(tc, endpoint_id, LOCAL_ID)
            tdata.add_item(globus_path + file_name, local_path + file_name + str(i))

            result = tc.submit_transfer(tdata)

            while not tc.task_wait(result["task_id"], polling_interval=1, timeout=60):
                print("waiting for download: {}".format(globus_path + file_name))


tc = get_globus_client()

with open("pub8_list.txt", "r") as file_list:
    save_readmes(tc, PETREL_ID, "/home/paul/", file_list.readlines(), 3066)
