import csv
from netCDF4 import Dataset
import ftplib
from hashlib import md5


# TODO: bounding box method for lat and lon lists, granularity of data

def get_metadata(file_name, path):
    """Create metadata JSON from file.

                            :param file_name: (str) file name
                            :param path: (str) path to file
                            :returns: (dict) metadata dictionary"""

    extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"

    file_handle = open(path + file_name, 'r')

    metadata = {
        "file": file_name,
        "path": path,
        "type": extension,
        "checksum": md5(file_handle.read()),
        "metadata": {}
    }

    if extension in ["csv", "txt"]:
        headers = get_headers(file_handle, extension)
        if headers:
            metadata["metadata"]["headers"] = headers
    elif extension == "nc":
        metadata["metadata"].update(get_netcdf_metadata(file_name, path))

    return metadata


def get_netcdf_metadata(file_name, path):
    """Create netcdf metadata JSON from file.

                                :param file_name: (str) file name
                                :param path: (str) path to file
                                :returns: (dict) metadata dictionary"""

    dataset = Dataset(path + file_name)
    metadata = {
        "file_format": dataset.file_format,
    }
    if len(dataset.ncattrs()) > 0:
        metadata["global_attributes"] = {}
    for attr in dataset.ncattrs():
        metadata["global_attributes"][attr] = dataset.getncattr(attr)

    dims = dataset.dimensions
    if len(dims) > 0:
        metadata["dimensions"] = {}
    for dim in dims:
        metadata["dimensions"][dim] = {
            "size": len(dataset.dimensions[dim])
        }
        add_ncattr_metadata(dataset, dim, "dimensions", metadata)

    vars = dataset.variables
    if len(vars) > 0:
        metadata["variables"] = {}
    for var in vars:
        if var not in dims:
            metadata["variables"][var] = {
                "name": var,
                "dimensions": dataset.variables[var].dimensions,
                "size": dataset.variables[var].size
            }
        add_ncattr_metadata(dataset, var, "variables", metadata)

    return metadata


def add_ncattr_metadata(dataset, name, dim_or_var, metadata):
    """Get attributes from a netCDF variable or dimension.

                        :param dataset: (netCDF4.Dataset) dataset from which to extract metadata
                        :param name: (str) name of attribute
                        :param dim_or_var: ("dimensions" | "variables") metadata key for attribute info
                        :param metadata: (dict) dictionary to add this attribute info to"""
    try:
        metadata[dim_or_var][name]["type"] = dataset.variables[name].dtype
        for attr in dataset.variables[name].ncattrs():
            metadata[dim_or_var][name][attr] = dataset.variables[name].getncattr(attr)
            # some variables have no attributes
    except KeyError:
        pass


def get_headers(file_handle, extension):
    """Get all header fields from file, return None if none can be retrieved.

                    :param file_handle: (file) open file
                    :param extension: (str) file extension used to determine csv.reader parameters
                    :returns: (list(str) | None)"""

    # TODO: determine if whitespace separation for non-csv is effective

    # choose csv.reader parameters based on file type - if not csv, try space-delimited
    if extension == "csv":
        reader = csv.reader(file_handle)
    else:
        reader = csv.reader(file_handle, delimiter=' ', skipinitialspace=True)

    headers = []

    for row in reader:
        # if the row is a header row, add all its fields to the all_fields list
        if is_header_row(row):
            headers += row
        else:
            break

    return headers if headers else None


def is_header_row(row):
    """Determine if row is a header row by checking if it contains any fields that are
    only numeric.

            :param row: (list(str)) list of fields in row
            :returns: (bool) whether row is a header row"""

    for field in row:
        try:
            float(field)
            return False
        except ValueError:
            pass
    return True