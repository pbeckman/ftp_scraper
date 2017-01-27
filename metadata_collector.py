import os
import json
import tarfile
from re import compile
from zipfile import ZipFile
from ftplib import error_perm
from metadata_util import get_metadata

# pattern used to distinguish files from directories - has '.' in 2nd, 3rd, or 4th to last character
file_pattern = compile("^.*\..{2,4}$")


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
    """Catalogs the name, path, size, and type of each file, writing it with the
    given catalog_writer if possible, and with the failure_writer if un-openable.

            :param ftp: (ftp.FTP) ftp handle
            :param metadata_file: (files) JSON file for metadata
            :param directory: (str) directory name
            :returns: (dict) aggregate file number and size data for each file extension"""

    # dictionary storing information that will populate the aggregate csv
    stats = {
        "total_bytes": 0,
        "total_bytes_with_metadata": 0,
        "total_files": 0,
        "total_files_with_metadata": 0
    }

    # corrects the path of the directory with '/' if necessary
    directory = (directory + '{}').format('/' if directory[-1] != '/' else '')

    # record current directory in order to later return to it
    working_directory = ftp.pwd()

    ftp.cwd(directory)
    print "collecting metadata from directory: " + directory

    # all items in current directory
    item_list = ftp.nlst()

    for item in item_list:
        if is_dir(ftp, item):
            # recursively catalog subdirectory and get its metadata stats
            new_stats = write_metadata(ftp, metadata_file, directory + item)
            # add subdirectory stats to total stats
            combine_stats(stats, new_stats)
            print stats
        else:
            # some items are corrupt or strange and can't be read, so skip them
            try:
                print "collecting metadata from item: " + item
                size = ftp.size(item)
                extension = item.split('.', 1)[1] if '.' in item else "no extension"
                metadata = {
                    "file": item,
                    "path": directory,
                    "type": extension,
                    "size": size
                }
                # TODO: add decompression step
                with_metadata = False
                if extension in ["csv", "txt", "nc"]:
                    with open("download/{}".format(item), 'wb') as f:
                        ftp.retrbinary('RETR {}'.format(item), f.write)
                    specific_metadata = get_metadata(item, "download/")
                    os.remove("download/{}".format(item))
                    if specific_metadata != {}:
                        metadata["metadata"] = specific_metadata
                        with_metadata = True
                metadata_file.write(json.dumps(metadata))
                # add data from this file to total stats
                stats["total_bytes"] += size
                stats["total_files"] += 1
                if with_metadata:
                    stats["total_bytes_with_metadata"] += size
                    stats["total_files_with_metadata"] += 1
            except error_perm:
                pass

    # pop back up to the original directory
    ftp.cwd(working_directory)

    return stats


def combine_stats(stats, new_stats):
    for key in stats.keys():
        stats[key] += new_stats[key]

# zip = ZipFile("test_files/compressed.zip")
# contents = zip.namelist()
#
# zip.extractall("test_files/compressed")
# for item in contents:
#     if os.path.isdir():
