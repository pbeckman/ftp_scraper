import sys
import re
from ftplib import FTP, error_perm
import csv

# TODO: don't rely on global variables for csv writers and aggregate data

ftp = FTP("cdiac.ornl.gov")
ftp.login()

catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])

agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
agg_writer.writerow(["file extension", "number of files", "average size (bytes)"])

fail_writer = csv.writer(open("cdiac_uncatalogued.csv", "w"))
fail_writer.writerow(["item name", "path"])

# pattern used to distinguish files from directories
file_pattern = re.compile("^.*\..{2,4}$")


def is_dir(item, guess_by_extension=True):
    """Determine if item is a directory.

        :param item: (str) item name
        :param guess_by_extension: (bool)
        whether to assume items with a '.' in characters -2, -3, or -4 are files
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


def write_catalog(directory):
    """Catalogs the name, path, size, and type of each file, writing it with the
    `catalog_writer` specified above

            :param directory: (str) item name
            :returns: (dict) aggregate file number and size data for each file extension"""
    # TODO: generalize this to take an output csv as a parameter?

    # dictionary storing information that will populate the aggregate csv
    agg_data = {}

    working_directory = ftp.pwd()

    ftp.cwd(directory)
    print "cataloging directory: " + directory

    # all items in current directory
    item_list = ftp.nlst()

    for item in item_list:
        # if the item is a directory, this will create the correct path to get to it
        sub_directory = (directory + '{}' + item).format('/' if directory[-1] != '/' else '')
        if is_dir(item):
            # recursively catalog subdirectory and get its aggregate data
            new_agg = write_catalog(sub_directory)
            # add subdirectory aggregate data to total aggregate data
            add_agg(agg_data, new_agg)
        else:
            try:
                print "cataloging item: " + item
                extension = item.split('.', 1)[1] if '.' in item else "no extension"
                size = ftp.size(sub_directory)
                catalog_writer.writerow([
                    item,
                    directory,
                    extension,
                    size
                ])
                # add data from this file to total aggregate data
                try:
                    agg_data[extension]["files"] += 1
                    agg_data[extension]["total_bytes"] += size
                except KeyError:
                    agg_data[extension] = {"files": 1, "total_bytes": size}
            except error_perm:
                fail_writer.writerow([item, directory])

    ftp.cwd(working_directory)

    return agg_data


def add_agg(parent_agg, new_agg):
    """Combine subdirectory aggregate data with parent aggregate data.

            :param parent_agg: (dict) aggregate data from parent directory
            :param new_agg: (dict) aggregate data from subdirectory
            whether to assume items with a '.' in characters -2, -3, or -4 are files
            this avoids the slower, more costly cwd command
            :returns: (dict) combined aggregate data"""

    for extension, extension_data in new_agg.iteritems():
        try:
            parent_agg[extension]["files"] += extension_data["files"]
            parent_agg[extension]["total_bytes"] += extension_data["total_bytes"]
        except KeyError:
            parent_agg[extension] = {
                "files": extension_data["files"],
                "total_bytes": extension_data["total_bytes"]
            }

    return parent_agg


def write_agg(data):
    """Write the aggregate data with the `agg_writer` specified above.

                :param data: (dict) aggregate data to write"""
    # TODO: generalize this to take an output csv as a parameter?

    for extension, extension_data in data.iteritems():
        agg_writer.writerow([
           extension,
           extension_data["files"],
           extension_data["total_bytes"]/extension_data["files"]
        ])


if __name__ == "__main__":
    # test: "/pub10/ushcn_snow/R_input/FL"
    write_agg(write_catalog(sys.argv[1]))

