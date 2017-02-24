from __future__ import print_function
import os
import json
import globus_sdk
from hashlib import sha256
from re import compile
from metadata_util import get_metadata

# pattern used to distinguish files from directories - has '.' in 2nd, 3rd, or 4th to last character
file_pattern = compile("^.*\..{2,4}$")

PETREL_ID = os.environ["PETREL_ID"]
LOCAL_ID = os.environ["LOCAL_ID"]

# create a client object that tracks state as we do this flow
client = globus_sdk.NativeAppAuthClient(LOCAL_ID)


def globus_first_login():
    # This method should only have to be run once EVER per user to get environment variables

    # explicitly start the flow (some clients may support multiple flows)
    client.oauth2_start_flow_native_app(refresh_tokens=True)
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


def get_globus_client():
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        os.environ["REFRESH_TOKEN"],
        client,
        access_token=os.environ["ACCESS_TOKEN"],
        expires_at=int(os.environ["EXPIRES_AT_SECONDS"]))

    # and try using `tc` to make TransferClient calls. Everything should just
    # work -- for days and days, months and months, even years
    tc = globus_sdk.TransferClient(authorizer=authorizer)

    return tc


def write_file_list(tc, endpoint_id, path, list_file):
    # corrects the path with '/' if necessary
    path = (path + '{}').format('/' if path[-1] != '/' else '')

    list = tc.operation_ls(endpoint_id, path=path)
    for item in list:
        item_path = path + item["name"]
        if item["type"] == "dir":
            write_file_list(tc, endpoint_id, item_path, list_file)
        elif item["type"] == "file":
            list_file.write(item_path + '\n')


def download_file(tc, endpoint_id, path, file_name):
    tdata = globus_sdk.TransferData(tc, endpoint_id, LOCAL_ID)
    tdata.add_item(path + file_name, "/home/paul/" + file_name)

    result = tc.submit_transfer(tdata)

    while not tc.task_wait(result["task_id"], polling_interval=1):
        print("waiting for file {} to download".format(file_name))

    print("download complete")
    print(result.data)


def write_metadata(tc, endpoint_id, files, start_file_number, path_to_endpoint, metadata_file, restart_file):
    for file_number in range(start_file_number, len(files)):
        full_file_name = files[file_number]
        globus_path, file_name = full_file_name.rsplit("/", 1)

        extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"
        # for null value collection only process these 3 types
        if extension in ["csv", "txt", "dat"]:
            metadata = get_file_metadata(tc, endpoint_id, globus_path, file_name, path_to_endpoint)
            # write metadata to file
            try:
                metadata_file.write(json.dumps(metadata) + ",")
            except Exception as e:
                with open("errors.txt", "w") as error_file:
                    error_file.write(full_file_name + ": error = " + str(e) + "\n")

        restart_file.truncate(0)
        restart_file.write("{},{}".format(file_number, full_file_name))


def get_file_metadata(tc, endpoint_id, globus_path, file_name, path_to_endpoint):
    download_file(tc, endpoint_id, globus_path, file_name)
    local_path_to_file = path_to_endpoint + file_name

    extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"
    metadata = {
        "file": file_name,
        "path": globus_path,
        "type": extension,
        "size": os.path.getsize(local_path_to_file)
    }

    content_metadata = get_metadata(file_name, local_path_to_file)

    os.remove(local_path_to_file)

    if content_metadata != {}:
        metadata["content_metadata"] = content_metadata

    return metadata


# get client
tc = get_globus_client()

# activate Petrel endpoint
tc.endpoint_autoactivate(PETREL_ID)

# with open("pub8_list.txt", "w") as f:
#     write_file_list(tc, PETREL_ID, "/cdiac/cdiac.ornl.gov/pub8/", f)

download_file(tc, PETREL_ID, "/cdiac/cdiac.ornl.gov/pub8/oceans/AMT_data/", "AMT1.txt")

