import csv
import json
from ftplib import FTP
from catalog_maker import write_agg, write_catalog
from metadata_util import get_metadata
from metadata_collector import write_metadata

ftp = FTP("cdiac.ornl.gov")
ftp.login()

# catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
# catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])
#
# agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
# agg_writer.writerow(["file type", "number of files", "total size (bytes)", "average size (bytes)"])
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
    print json.dumps(get_metadata(file_name, path), sort_keys=True, indent=4, separators=(',', ': '))

# # test metadata collection
# display_metadata("some_netcdf.nc", "test_files/")
# display_metadata("single_header.csv", "test_files/")
# display_metadata("readme.txt", "test_files/")
# display_metadata("multiple_headers.csv", "test_files/")
# display_metadata("single_header.txt", "test_files/")

# file for all JSON metadata
with open("metadata.json", "w") as metadata_file:
    with open("stats.txt", "w") as stats_file:
        stats_file.write(write_metadata(ftp, metadata_file, "/pub8/"))

