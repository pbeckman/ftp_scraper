from __future__ import print_function
import os
import csv
import json
import globus_sdk
from hashlib import sha256
from re import compile
from metadata_util import get_metadata

# pattern used to distinguish files from directories - has '.' in 2nd, 3rd, or 4th to last character
file_pattern = compile("^.*\..{2,4}$")

PETREL_ID = os.environ["PETREL_ID"]
LOCAL_ID = os.environ["LOCAL_ID"]
TRANSFER_TOKEN = os.environ["TRANSFER_TOKEN"]

# create a client object that tracks state as we do this flow
client = None  # globus_sdk.NativeAppAuthClient(LOCAL_ID)


def globus_first_login():
    # This method should only have to be run once EVER per user to get refresh token environment variable

    # explicitly start the flow (some clients may support multiple flows)
    client.oauth2_start_flow_native_app()  # refresh_tokens=True
    # print URL
    print("Login Here:\n{0}".format(client.oauth2_get_authorize_url()))

    auth_code = raw_input("\nEnter authentication code:\n").strip()

    # exchange auth_code for a response object containing your token(s)
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # let's get stuff for the Globus Transfer service
    globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']
    # print and assign environment variables
    print("\nREFRESH_TOKEN=" + str(globus_transfer_data['refresh_token']))
    os.environ["REFRESH_TOKEN"] = globus_transfer_data['refresh_token']
    print("ACCESS_TOKEN=" + str(globus_transfer_data['access_token']))
    os.environ["ACCESS_TOKEN"] = globus_transfer_data['access_token']
    print("EXPIRES_AT_SECONDS=" + str(globus_transfer_data['expires_at_seconds']))
    os.environ["EXPIRES_AT_SECONDS"] = globus_transfer_data['expires_at_seconds']

    transfer_authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_data)

    # create a TransferClient object which Authorizes its calls using that GlobusAuthorizer
    tc = globus_sdk.TransferClient(authorizer=transfer_authorizer)

    return tc


def get_globus_client():
    # authorizer = globus_sdk.RefreshTokenAuthorizer(
    #     os.environ["REFRESH_TOKEN"],
    #     client)

    authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)

    # and try using `tc` to make TransferClient calls. Everything should just
    # work -- for days and days, months and months, even years
    tc = globus_sdk.TransferClient(authorizer=authorizer)

    return tc


def write_file_list(tc, endpoint_id, globus_path, list_file):
    # corrects the path with '/' if necessary
    globus_path = (globus_path + '{}').format('/' if globus_path[-1] != '/' else '')

    list = tc.operation_ls(endpoint_id, path=globus_path)
    for item in list:
        item_path = globus_path + item["name"]
        if item["type"] == "dir":
            write_file_list(tc, endpoint_id, item_path, list_file)
        elif item["type"] == "file":
            list_file.write(item_path + '\n')


def download_file(tc, endpoint_id, globus_path, file_name, local_path):
    print("downloading file {}".format(globus_path + file_name))
    tdata = globus_sdk.TransferData(tc, endpoint_id, LOCAL_ID)
    tdata.add_item(globus_path + file_name, local_path + file_name)

    result = tc.submit_transfer(tdata)

    while not tc.task_wait(result["task_id"], polling_interval=1, timeout=60):
        print("waiting for download: {}".format(globus_path + file_name))


def delete_file(tc, local_path, file_name):
    print("deleting file {}".format(local_path + file_name))
    ddata = globus_sdk.DeleteData(tc, LOCAL_ID)

    ddata.add_item(local_path + file_name)

    # # Ensure endpoint is activated
    # tc.endpoint_autoactivate(endpoint_id)

    result = tc.submit_delete(ddata)


def write_metadata(tc, endpoint_id, files, start_file_number, local_path, csv_writer, restart_file):
    for file_number in range(start_file_number, len(files)):
        full_file_name = files[file_number]
        globus_path, file_name = full_file_name.strip().rsplit("/", 1)
        globus_path += "/"

        extension = file_name.split('.', 1)[1].strip() if '.' in file_name else "no extension"
        # for null value collection only process these 3 types
        if extension in ["csv", "txt", "dat"]:
            metadata = get_file_metadata(tc, endpoint_id, globus_path, file_name, local_path)

            # write metadata to file if there are aggregates
            if "content_metadata" in metadata.keys() and len(metadata["content_metadata"].keys()) > 1:
                print("writing to col_metadata.csv:")
                print(metadata)
                write_dict_to_csv(metadata, csv_writer)

        restart_file.write("{},{}".format(file_number, full_file_name))


def get_file_metadata(tc, endpoint_id, globus_path, file_name, local_path):
    print("collecting metadata from {}".format(globus_path + file_name))
    download_file(tc, endpoint_id, globus_path, file_name, local_path)
    local_path_to_file = local_path + file_name

    extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"
    metadata = {
        "file": file_name,
        "path": globus_path,
        "type": extension,
        "size": os.path.getsize(local_path_to_file)
    }

    content_metadata = get_metadata(file_name, local_path)

    delete_file(tc, local_path, file_name)

    if content_metadata != {}:
        metadata["content_metadata"] = content_metadata

    return metadata


def write_dict_to_csv(metadata, csv_writer):
    cols = metadata["content_metadata"].keys()
    cols.remove("headers")
    for col in cols:
        col_agg = metadata["content_metadata"][col]
        print(col_agg)
        print(col_agg.keys())
        csv_writer.writerow([
            metadata["path"], metadata["file"], col,

            col_agg["min"][0] if "min" in col_agg.keys() else None,
            col_agg["min"][1] - col_agg["min"][0] if "min" in col_agg.keys() and len(col_agg["min"]) > 1 else None,
            col_agg["min"][1] if "min" in col_agg.keys() and len(col_agg["min"]) > 1 else None,
            col_agg["min"][2] - col_agg["min"][1] if "min" in col_agg.keys() and len(col_agg["min"]) > 2 else None,
            col_agg["min"][2] if "min" in col_agg.keys() and len(col_agg["min"]) > 2 else None,

            col_agg["max"][0] if "max" in col_agg.keys() else None,
            col_agg["max"][0] - col_agg["max"][1] if "max" in col_agg.keys() and len(col_agg["max"]) > 1 else None,
            col_agg["max"][1] if "max" in col_agg.keys() and len(col_agg["max"]) > 1 else None,
            col_agg["max"][1] - col_agg["max"][2] if "max" in col_agg.keys() and len(col_agg["max"]) > 2 else None,
            col_agg["max"][2] if "max" in col_agg.keys() and len(col_agg["max"]) > 2 else None,

            col_agg["avg"] if "avg" in col_agg.keys() else None,
            col_agg["mode"] if "mode" in col_agg.keys() else None,

            None, None, None  # spaces for null values to be recorded by hand
        ])

# get client
tc = get_globus_client()

# # activate Petrel endpoint
# tc.endpoint_autoactivate(PETREL_ID)
#
# # activate local endpoint
# tc.endpoint_autoactivate(LOCAL_ID)

# with open("pub8_list.txt", "w") as f:
#     write_file_list(tc, PETREL_ID, "/cdiac/cdiac.ornl.gov/pub8/", f)

# download_file(tc, PETREL_ID, "/cdiac/cdiac.ornl.gov/pub8/oceans/AMT_data/", "AMT1.txt")

# print(get_file_metadata(tc, PETREL_ID, "/cdiac/cdiac.ornl.gov/pub8/oceans/AMT_data/", "AMT1.txt", "/home/paul/"))

csv_writer = csv.writer(open("col_metadata.csv", "a"))
csv_writer.writerow([
    "path", "file", "column",
    "min_1", "min_diff_1", "min_2", "min_diff_1", "min_3",
    "max_1", "max_diff_1", "max_2", "max_diff_1", "max_3",
    "avg", "mode",
    "null_1", "null_2", "null_3"
])

with open("pub8_list.txt", "r") as file_list:
    with open("restart.txt", "a") as restart_file:
        write_metadata(tc, PETREL_ID, file_list.readlines(), 0, "/home/paul/", csv_writer, restart_file)

# metadata = get_metadata("single_header.csv", "test_files/")
# write_dict_to_csv(metadata, csv_writer)
