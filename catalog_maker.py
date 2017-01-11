import sys
import re
from ftplib import error_perm
import csv

from metadata_collector import collect_metadata

# pattern used to distinguish files from directories - has '.' in 2nd, 3rd, or 4th to last character
file_pattern = re.compile("^.*\..{2,4}$")


def is_dir(ftp, item, guess_by_extension=True):
    """Determine if item is a directory.

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


def write_catalog(ftp, directory, catalog_writer, failure_writer):
    """Catalogs the name, path, size, and type of each file, writing it with the
    `catalog_writer` specified above

            :param ftp: (ftp.FTP) ftp handle
            :param directory: (str) item name
            :param catalog_writer: (csv.writer) writer used to catalog all valid items in the directory
            headers = "filename", "path", "file type", "size (bytes)"
            :param failure_writer: (csv.writer) writer used to catalog all un-openable items in the directory
            headers = "item name", "path"
            :returns: (dict) aggregate file number and size data for each file extension"""

    # dictionary storing information that will populate the aggregate csv
    agg_data = {}

    # record current directory in order to later return to it
    working_directory = ftp.pwd()

    ftp.cwd(directory)
    print "cataloging directory: " + directory

    # all items in current directory
    item_list = ftp.nlst()

    for item in item_list:
        # if the item is a directory, this will create the correct path to get to it
        sub_directory = (directory + '{}' + item).format('/' if directory[-1] != '/' else '')
        if is_dir(ftp, item):
            # recursively catalog subdirectory and get its aggregate data
            new_agg = write_catalog(ftp, sub_directory, catalog_writer, failure_writer)
            # add subdirectory aggregate data to total aggregate data
            combine_agg(agg_data, new_agg)
        else:
            # some items are corrupt or strange and can't be read, so throw those into a "failure" csv
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
                failure_writer.writerow([item, directory])

    # pop back up to the original directory
    ftp.cwd(working_directory)

    return agg_data


def combine_agg(parent_agg, new_agg):
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


def write_agg(data, agg_writer):
    """Write the aggregate data with the `agg_writer` specified above.

                :param data: (dict) aggregate data to write
                :param agg_writer: (csv.writer) writer used to write aggregates
                headers = "file extension", "number of files", "total size (bytes)", "average size (bytes)" """

    for extension, extension_data in data.iteritems():
        agg_writer.writerow([
            extension,
            extension_data["files"],
            extension_data["total_bytes"],
            extension_data["total_bytes"] / extension_data["files"]
        ])


if __name__ == "__main__":
    pass
