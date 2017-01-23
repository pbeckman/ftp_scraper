import csv
import json
from ftplib import FTP
from catalog_maker import write_agg, write_catalog
from metadata_collector import get_metadata

ftp = FTP("cdiac.ornl.gov")
ftp.login()

catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])

agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
agg_writer.writerow(["file type", "number of files", "total size (bytes)", "average size (bytes)"])

failure_writer = csv.writer(open("cdiac_uncatalogued.csv", "w"))
failure_writer.writerow(["item name", "path"])

# # test writing aggregates and catalogs
# write_agg(
#     write_catalog(ftp, "/pub2/ndp026c/", catalog_writer, failure_writer),
#     agg_writer)

# # test collecting metadata
print """
----------------------------
some_netcdf.nc
----------------------------
"""
print json.dumps(get_metadata("some_netcdf.nc", "test_files/"), sort_keys=True, indent=4, separators=(',', ': '))

print """
----------------------------
single_header.csv
----------------------------
"""
print json.dumps(get_metadata("single_header.csv", "test_files/"), sort_keys=True, indent=4, separators=(',', ': '))

print """
----------------------------
multiple_headers.csv
----------------------------
"""
print json.dumps(get_metadata("multiple_headers.csv", "test_files/"), sort_keys=True, indent=4, separators=(',', ': '))

print """
----------------------------
single_header.txt
----------------------------
"""
print json.dumps(get_metadata("single_header.txt", "test_files/"), sort_keys=True, indent=4, separators=(',', ': '))
