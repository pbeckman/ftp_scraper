import csv
import json
import numpy
from netCDF4 import Dataset
import ftplib
from hashlib import md5


# TODO: bounding box method for lat and lon lists, granularity of data
# TODO: add failure cases to determine if a file is columnar or not

def get_metadata(file_name, path):
    """Create metadata JSON from file.

        :param file_name: (str) file name
        :param path: (str) path to file
        :returns: (dict) metadata dictionary"""

    with open(path + file_name, 'rU') as file_handle:

        extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"

        metadata = {
            "file": file_name,
            "path": path,
            "type": extension,
            "checksum": md5(file_handle.read()).hexdigest()
        }

        # go back to the beginning of the file for real processing,
        # because the checksum put the cursor at the end
        file_handle.seek(0)

        if extension in ["csv", "txt"]:
            specific_metadata = get_columnar_metadata(file_handle, extension)
        elif extension == "nc":
            specific_metadata = get_netcdf_metadata(file_name, path)
        else:
            specific_metadata = {}

        if specific_metadata != {}:
            metadata["metadata"] = specific_metadata

    return metadata


class NumpyDecoder(json.JSONEncoder):
    """Serializer used to convert numpy dtypes to normal json serializable types.
    Since netCDF4 produces numpy types, this is necessary for compatibility with
    other metadata scrapers like the csv, which returns a python dict"""

    def default(self, obj):
        if isinstance(obj, numpy.generic):
            return numpy.asscalar(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, numpy.dtype):
            return str(obj)
        else:
            return super(NumpyDecoder, self).default(obj)


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
                "dimensions": dataset.variables[var].dimensions,
                "size": dataset.variables[var].size
            }
        add_ncattr_metadata(dataset, var, "variables", metadata)

    # cast all numpy types to native python types via dumps, then back to dict via loads
    return json.loads(json.dumps(metadata, cls=NumpyDecoder))


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


def get_columnar_metadata(file_handle, extension):
    """Get all header fields from file, return None if none can be retrieved.

        :param file_handle: (file) open file
        :param extension: (str) file extension used to determine csv.reader parameters
        :returns: (dict) ascertained metadata"""

    # TODO: determine if whitespace separation for non-csv is effective, and if comma and excel dialect are for csv

    # choose csv.reader parameters based on file type - if not csv, try space-delimited
    if extension == "csv":
        reader = csv.reader(file_handle, skipinitialspace=True)
    else:
        reader = SpaceDelimitedReader(file_handle)

    headers = []
    header_types = []
    metadata = {}
    num_value_rows = 0
    num_header_rows = 0
    # this shows that we are on the first row and should type check
    # to decide whether to use numeric or text aggregation
    first_value_row = True

    for row in reader:
        # if the row is a header row, add all its fields to the headers list
        if is_header_row(row):
            num_header_rows += 1
            for header in row:
                if header != "":
                    headers.append(header)

        # don't return column aggregate data for files with multiple header rows
        elif num_header_rows == 1:
            # type check the first row to decide which aggregates to use
            if first_value_row:
                header_types = ["num" if is_number(field) else "str" for field in row]

            # add row data to aggregates
            for i in range(0, len(row)):
                num_value_rows += 1
                value = row[i]
                header = headers[i]
                header_type = header_types[i]

                if header_type == "num":
                    # cast the field to a number to do numerical aggregates
                    value = float(value)

                    # start of the metadata if this is the first row of values
                    if first_value_row:
                        metadata[header] = {
                            "min": value,
                            "max": value,
                            "total": value,
                        }

                    # add row data to existing aggregates
                    else:
                        if value < metadata[header]["min"]:
                            metadata[header]["min"] = value
                        if value > metadata[header]["max"]:
                            metadata[header]["max"] = value
                        metadata[header]["total"] += value

                elif header_type == "str":
                    # TODO: add string field aggregates?
                    pass

            first_value_row = False

    # add header list to metadata
    if len(headers) > 0:
        metadata["headers"] = headers
    # calculate averages for numerical columns if aggregates were taken,
    # (which only happends when there is a single row of headers)
    if num_header_rows == 1:
        for i in range(0, len(headers)):
            if header_types[i] == "num":
                header = headers[i]
                metadata[header]["avg"] = metadata[header]["total"] / num_value_rows
                metadata[header].pop("total")

    return metadata


class SpaceDelimitedReader:
    """Reader for space delimited files. Acts in the same way as the standard csv.reader

    :param file_handle: (file) open file """

    def __init__(self, file_handle):
        self.fh = file_handle
        self.dialect = ""
        self.line_num = 0

    def next(self):
        fields = []
        line = self.fh.readline()
        if line == "":
            raise StopIteration
        for field in line.split(" "):
            if field.strip() != "":
                fields.append(field.strip())
        self.line_num += 1
        return fields

    def __iter__(self):
        return self


def is_header_row(row):
    """Determine if row is a header row by checking if it contains any fields that are
    only numeric.

        :param row: (list(str)) list of fields in row
        :returns: (bool) whether row is a header row"""

    for field in row:
        if is_number(field):
            return False
    return True


def is_number(field):
    """Determine if a string is a number by attempting to cast to it a float.

        :param field: (str) field
        :returns: (bool) whether field can be cast to a number"""

    try:
        float(field)
        return True
    except ValueError:
        return False
