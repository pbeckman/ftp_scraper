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

    wd = ftp.pwd()  # current working directory
    try:
        ftp.cwd('/')
        ftp.cwd(item)  # see if name represents a child-subdirectory
        ftp.cwd(wd)    # It works! Go back. Continue DFS.
        return True
    except:
        return False


def make_catalog(dir_path):
    ftp.cwd('/')
    ftp.cwd(dir_path)
    print "cataloging directory: " + dir_path

    item_list = ftp.nlst()

    for item in item_list:
        if is_dir(dir_path + item):
            make_catalog(dir_path + item)
        else:
            item_path = dir_path + '/' + item
            writer.writerow([
                item,
                dir_path,
                item.split('.', 1)[1] if '.' in item else '',
                ftp.size(item_path)
            ])

make_catalog("/pub10/ushcn_snow/R_input/")
