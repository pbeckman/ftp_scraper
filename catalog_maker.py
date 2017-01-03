import re
from ftplib import *
import csv

ftp = FTP("cdiac.ornl.gov")
ftp.login()

writer = csv.writer(open("output.csv", "w"))
writer.writerow(["filename", "path", "file extension", "size (bytes)"])

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
            item_path = directory + '/' + item
            writer.writerow([
                item,
                directory,
                item.split('.', 1)[1] if '.' in item else '',
                ftp.size(item_path)
            ])

    ftp.cwd(working_directory)

make_catalog("/pub10/ushcn_snow/R_input/")
