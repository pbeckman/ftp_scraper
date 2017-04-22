import csv
import json
import os
from ftplib import FTP
# from catalog_maker import write_agg, write_catalog
from metadata_util import extract_metadata, extract_netcdf_metadata

# ftp = FTP("cdiac.ornl.gov")
# ftp.login()

# catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
# catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])
#
# agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
# agg_writer.writerow(["file type", "total size (bytes)", "total size with metadata (bytes)"])
#
# failure_writer = csv.writer(open("cdiac_uncatalogued.csv", "w"))
# failure_writer.writerow(["item name", "path"])


# # test writing aggregates and catalogs
# write_agg(
#     write_catalog(ftp, "/pub2/ndp026c/", catalog_writer, failure_writer),
#     agg_writer)


def display_metadata(file_name, path):
    print """
    ----------------------------
    {}
    ----------------------------
    """.format(path + file_name)
    print json.dumps(extract_metadata(file_name, path), sort_keys=True, indent=4, separators=(',', ': '))


def test_metadata_extraction():
    display_metadata("no_headers.csv", "test_files/")
    display_metadata("some_netcdf.nc", "test_files/")
    display_metadata("single_header.csv", "test_files/")
    display_metadata("readme.txt", "test_files/")
    display_metadata("multiple_headers.csv", "test_files/")
    display_metadata("single_header.txt", "test_files/")
    display_metadata("preamble.exc.csv", "test_files/")
    display_metadata("preamble.dat", "test_files/")
    display_metadata("preamble.c32", "test_files/")


def write_agg_csv(agg_writer, agg):
    for extension, extension_data in agg.iteritems():
        agg_writer.writerow([extension, extension_data["total_bytes"], extension_data["total_bytes_with_metadata"]])

test_metadata_extraction()
