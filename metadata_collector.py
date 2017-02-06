import os
import json
from hashlib import sha256
from re import compile
from ftplib import error_perm
from metadata_util import get_metadata

# pattern used to distinguish files from directories - has '.' in 2nd, 3rd, or 4th to last character
file_pattern = compile("^.*\..{2,4}$")

# TODO: restart point
# TODO: where to put try excepts...


def is_dir(ftp, item, guess_by_extension=True):
    """Determine if FTP item is a directory.

        :param ftp: (ftp.FTP) ftp handle
        :param item: (str) item name
        :param guess_by_extension: (bool)
        whether to assume items matching file_pattern are files
        this avoids the slower, more costly cwd command
        :returns: (bool) whether item is a directory"""

    if guess_by_extension is True and file_pattern.match(item):
        return False

    # current working directory
    working_directory = ftp.pwd()

    try:
        # see if item is a directory - directory change will fail if it is a file
        ftp.cwd(item)
        ftp.cwd(working_directory)
        return True
    except error_perm:
        return False


def write_metadata(ftp, metadata_file, directory):
    """Catalogs the name, path, size, and type of each file, along with any metadata we
    can collect, writing JSON to the metadata_file.

            :param ftp: (ftp.FTP) ftp handle
            :param metadata_file: (files) JSON file for metadata
            :param directory: (str) directory name
            :returns: (dict) aggregate file number and size data for each file extension"""

    # dictionary storing information that will populate the aggregate csv
    agg_data = {}

    # corrects the path of the directory with '/' if necessary
    directory = (directory + '{}').format('/' if directory[-1] != '/' else '')

    # record current directory in order to later return to it
    working_directory = ftp.pwd()

    ftp.cwd(directory)
    # print "collecting metadata from directory: " + directory

    # all items in current directory
    item_list = ftp.nlst()

    for item in item_list:
        if is_dir(ftp, item):
            # recursively catalog subdirectory and get its metadata stats
            new_agg_data = write_metadata(ftp, metadata_file, directory + item)
            # add subdirectory stats to total stats
            combine_agg(agg_data, new_agg_data)
            # print stats
        else:
            # some items are corrupt or strange and can't have htier size collected, so skip them
            try:
                print "collecting metadata from item: " + item
                extension = item.split('.', 1)[1] if '.' in item else "no extension"
                metadata = {
                    "file": item,
                    "path": directory,
                    "type": extension,
                }

                # if we might be able to get real metadata from this file, download it
                try:
                    local_path_to_item = "download/{}".format(item)
                    with open(local_path_to_item, 'wb') as f:
                        ftp.retrbinary('RETR {}'.format(item), f.write)

                        metadata["size"] = os.path.getsize(local_path_to_item)
                        metadata["checksum"] = sha256(open(local_path_to_item, 'rb').read()).hexdigest()

                        content_metadata = get_metadata(item, "download/")

                        # add data from this file to total aggregate data
                        try:
                            agg_data[extension]["total_bytes"] += metadata["size"]
                        except KeyError:
                            agg_data[extension] = {
                                "total_bytes": metadata["size"],
                                "total_bytes_with_metadata": 0
                            }

                        if content_metadata != {}:
                            metadata["content_metadata"] = content_metadata
                            agg_data[extension]["total_bytes_with_metadata"] += metadata["size"]

                        # write metadata to file
                        try:
                            metadata_file.write(json.dumps(metadata) + ",")
                        except Exception as e:
                            with open("errors.txt", "w") as error_file:
                                error_file.write(directory + item + ":(a) error = " + str(e) + "\n")

                    os.remove(local_path_to_item)
                except Exception as e:
                    with open("errors.txt", "w") as error_file:
                        error_file.write(directory + item + ":(b) error = " + str(e) + "\n")

            except Exception as e:  # error_perm if size cannot be read
                with open("errors.txt", "w") as error_file:
                    error_file.write(directory + item + ":(c) error = " + str(e) + "\n")
                pass

    # pop back up to the original directory
    ftp.cwd(working_directory)

    return agg_data


def combine_agg(parent_agg, new_agg):
    """Combine subdirectory aggregate data with parent aggregate data.

            :param parent_agg: (dict) aggregate data from parent directory
            :param new_agg: (dict) aggregate data from subdirectory
            :returns: (dict) combined aggregate data"""

    for extension, extension_data in new_agg.iteritems():
        try:
            parent_agg[extension]["total_bytes"] += extension_data["total_bytes"]
            parent_agg[extension]["total_bytes_with_metadata"] += extension_data["total_bytes_with_metadata"]
        except KeyError:
            parent_agg[extension] = {
                "total_bytes": extension_data["total_bytes"],
                "total_bytes_with_metadata": extension_data["total_bytes_with_metadata"]
            }

    return parent_agg
