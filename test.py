import csv
from ftplib import FTP
from catalog_maker import write_agg, write_catalog
from metadata_collector import collect_metadata

ftp = FTP("cdiac.ornl.gov")
ftp.login()

catalog_writer = csv.writer(open("cdiac_catalog.csv", "w"))
catalog_writer.writerow(["filename", "path", "file type", "size (bytes)"])

agg_writer = csv.writer(open("cdiac_aggregates.csv", "w"))
agg_writer.writerow(["file extension", "number of files", "total size (bytes)", "average size (bytes)"])

failure_writer = csv.writer(open("cdiac_uncatalogued.csv", "w"))
failure_writer.writerow(["item name", "path"])

# test writing aggregates and catalogs
# write_agg(
#     write_catalog(ftp, "/pub2/ndp026c/", catalog_writer, failure_writer),
#     agg_writer)

# test collecting metadata
print collect_metadata(ftp, "BIOZAIRE3_hy1.csv", "/pub6/oceans/BIOZAIRE3/", "csv")
