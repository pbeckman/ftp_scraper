import sys
import re
from ftplib import FTP, error_perm
import csv

ftp = FTP("cdiac.ornl.gov")
ftp.login()

catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])

agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
agg_writer.writerow(["file extension", "number of files", "average size (bytes)"])

fail_writer = csv.writer(open("cdiac_uncatalogued.csv", "w"))
fail_writer.writerow(["item name", "path"])

file_pattern = re.compile("^.*\..{2,4}$")  # pattern used to distinguish files from directories


def is_dir(item, guess_by_extension=True):
    if guess_by_extension is True and file_pattern.match(item):
        return False

    working_directory = ftp.pwd()  # current working directory
    try:
        ftp.cwd(item)  # see if name represents a child-subdirectory
        ftp.cwd(working_directory)    # It works! Go back. Continue DFS.
        return True
    except:
        return False


def write_catalog(directory):
    agg_data = {}  # dictionary storing information that will populate the aggregate csv

    working_directory = ftp.pwd()
    print "current directory: " + directory

    ftp.cwd(directory)
    print "cataloging directory: " + directory

    item_list = ftp.nlst()

    for item in item_list:
        sub_directory = (directory + '{}' + item).format('/' if directory[-1] != '/' else '')
        if is_dir(item):
            new_agg = write_catalog(sub_directory)
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
    for extension, extension_data in data.iteritems():
        agg_writer.writerow([
           extension,
           extension_data["files"],
           extension_data["total_bytes"]/extension_data["files"]
        ])


if __name__ == "__main__":
    # test: "/pub10/ushcn_snow/R_input/FL"
    write_agg(write_catalog(sys.argv[1]))

