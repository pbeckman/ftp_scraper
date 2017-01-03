import re
from ftplib import *
import csv

ftp = FTP("cdiac.ornl.gov")
ftp.login()

catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])

agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
agg_writer.writerow(["file extension", "number of files", "average size (bytes)"])

agg_data = {}  # dictionary storing information that will populate the aggregate csv

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


def make_catalog(directory):
    working_directory = ftp.pwd()

    ftp.cwd(directory)
    print "cataloging directory: " + directory

    item_list = ftp.nlst()

    for item in item_list:
        if is_dir(item):
            make_catalog(directory + item)
        else:
            file_extension = item.split('.', 1)[1] if '.' in item else "no extension"
            size = ftp.size(directory + '/' + item)
            catalog_writer.writerow([
                item,
                directory,
                file_extension,
                size
            ])
            try:
                agg_data[file_extension]["files"] += 1
                agg_data[file_extension]["total_bytes"] += size
            except KeyError:
                agg_data[file_extension] = {"files": 1, "total_bytes": size}

    ftp.cwd(working_directory)

make_catalog("/pub10/ushcn_snow/R_input/")

for file_extension, extension_data in agg_data.iteritems():
    agg_writer.writerow([
        file_extension,
        extension_data["files"],
        extension_data["total_bytes"]/extension_data["files"]
    ])


