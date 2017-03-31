import csv
import json
import numpy
import os
from netCDF4 import Dataset
from decimal import Decimal
from operator import itemgetter


# TODO: bounding box method for lat and lon lists?
# TODO: granularity of data?


class ExtractionError(Exception):
    """Basic error to throw when an extractor fails"""


def get_metadata(file_name, path):
    """Create metadata JSON from file.

        :param file_name: (str) file name
        :param path: (str) absolute or relative path to file
        :returns: (dict) metadata dictionary"""

    with open(path + file_name, 'rU') as file_handle:

        extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"

        metadata = {}

        try:
            if extension in ["csv", "txt"]:
                metadata = get_columnar_metadata(file_handle)
        except ExtractionError:
            # not a columnar file
            pass

        if extension == "nc":
            metadata = get_netcdf_metadata(file_handle)

    return metadata


def get_netcdf_metadata(file_handle):
    """Create netcdf metadata JSON from file.

        :param file_handle: (str) file
        :returns: (dict) metadata dictionary"""

    dataset = Dataset(os.path.realpath(file_handle.name))
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


class NumpyDecoder(json.JSONEncoder):
    """Serializer used to convert numpy types to normal json serializable types.
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


def get_columnar_metadata(file_handle):
    """Get metadata from column-formatted file.

        :param file_handle: (file) open file
        :returns: (dict) ascertained metadata
        :raises: (ExtractionError) if the file cannot be read as a columnar file"""

    extension = file_handle.name.split('.', 1)[1] if '.' in file_handle.name else "no extension"

    # choose csv.reader parameters based on file type - if not csv, use space-delimited
    if extension in ["csv", "exc.csv"]:
        reader = csv.reader(file_handle, skipinitialspace=True)
    else:
        reader = SpaceDelimitedReader(file_handle)

    # base dictionary in which to store all the metadata
    metadata = {"columns": {}}

    headers = []
    col_types = []
    col_aliases = []
    num_value_rows = 0
    num_header_rows = 0
    # used to check if all rows are the same length, if not, this is not a valid columnar file
    row_length = 0
    first_row = True

    for row in reader:
        # if row is not the same length as previous row, raise an error showing this is not a valid columnar file
        if not first_row and row_length != len(row):
            raise ExtractionError
        first_row = False
        # update row length for next check
        row_length = len(row)

        # make column aliases so that we can create aggregates even for unlabelled columns
        col_aliases = ["__{}__".format(i) for i in range(0, row_length)]

        # if the row is a header row, add all its fields to the headers list
        if is_header_row(row):
            num_header_rows += 1
            for header in row:
                if header != "":
                    headers.append(header)

        else:
            # set the column aliases to the headers if they are 1-to-1 (single header row)
            if num_header_rows == 1:
                col_aliases = headers

            num_value_rows += 1

            # type check the first row to decide which aggregates to use
            if num_value_rows == 1:
                col_types = ["num" if is_number(field) else "str" for field in row]

            add_row_to_aggregates(metadata, row, col_aliases, col_types, num_value_rows == 1)

    # add header list to metadata
    if len(headers) > 0:
        metadata["headers"] = headers

    add_final_aggregates(metadata, col_aliases, col_types, num_value_rows)

    return metadata


def add_row_to_aggregates(metadata, row, col_aliases, col_types, is_first_value_row):
    """Adds row data to aggregates.

        :param metadata: (dict) metadata dictionary to add to
        :param row: (list(str)) row of strings to add
        :param col_aliases: (list(str)) list of headers
        :param col_types: (list("num" | "str")) list of header types
        :param is_first_value_row: (bool) whether this is the first value row, so we need to initialize
        the necessary aggregate dictionary in the metadata"""

    for i in range(0, len(row)):
        value = row[i]
        col_alias = col_aliases[i]
        col_type = col_types[i]

        if is_first_value_row:
            metadata["columns"][col_alias] = {}
            metadata["columns"][col_alias]["frequencies"] = {str(value): 1}
        else:
            if str(value) in metadata["columns"][col_alias]["frequencies"].keys():
                metadata["columns"][col_alias]["frequencies"][str(value)] += 1
            else:
                metadata["columns"][col_alias]["frequencies"][str(value)] = 1

        if col_type == "num":
            # cast the field to a number to do numerical aggregates
            value = float(value)

            # start off the metadata if this is the first row of values
            if is_first_value_row:
                metadata["columns"][col_alias]["min"] = [float("inf"), float("inf"), float("inf")]
                metadata["columns"][col_alias]["max"] = [None, None, None]
                metadata["columns"][col_alias]["total"] = value

            # add row data to existing aggregates
            else:
                if value < metadata["columns"][col_alias]["min"][0]:
                    metadata["columns"][col_alias]["min"][1:2] = metadata["columns"][col_alias]["min"][0:1]
                    metadata["columns"][col_alias]["min"][0] = value
                elif value < metadata["columns"][col_alias]["min"][1] \
                        and value != metadata["columns"][col_alias]["min"][0]:
                    metadata["columns"][col_alias]["min"][2] = metadata["columns"][col_alias]["min"][1]
                    metadata["columns"][col_alias]["min"][1] = value
                elif value < metadata["columns"][col_alias]["min"][2] \
                        and value not in metadata["columns"][col_alias]["min"][:2]:
                    metadata["columns"][col_alias]["min"][2] = value
                if value > metadata["columns"][col_alias]["max"][0]:
                    metadata["columns"][col_alias]["max"][1:2] = metadata["columns"][col_alias]["max"][0:1]
                    metadata["columns"][col_alias]["max"][0] = value
                elif value > metadata["columns"][col_alias]["max"][1] \
                        and value != metadata["columns"][col_alias]["max"][0]:
                    metadata["columns"][col_alias]["max"][2] = metadata["columns"][col_alias]["max"][1]
                    metadata["columns"][col_alias]["max"][1] = value
                elif value > metadata["columns"][col_alias]["max"][2] \
                        and value not in metadata["columns"][col_alias]["max"][:2]:
                    metadata["columns"][col_alias]["max"][2] = value
                metadata["columns"][col_alias]["total"] += value

        elif col_type == "str":
            # TODO: add string-specific field aggregates?
            pass


def add_final_aggregates(metadata, col_aliases, col_types, num_value_rows):
    """Adds row data to aggregates.

        :param metadata: (dict) metadata dictionary to add to
        :param col_aliases: (list(str)) list of headers
        :param col_types: (list("num" | "str")) list of header types
        :param num_value_rows: (int) number of value rows"""

    # calculate averages for numerical columns if aggregates were taken,
    # (which only happens when there is a single row of headers)
    for i in range(0, len(col_aliases)):
        col_alias = col_aliases[i]
        metadata["columns"][col_alias]["mode"] = max(metadata["columns"][col_alias]["frequencies"].iteritems(),
                                                     key=itemgetter(1))[0]
        metadata["columns"][col_alias].pop("frequencies")

        if col_types[i] == "num":
            metadata["columns"][col_alias]["max"] = [val for val in metadata["columns"][col_alias]["max"]
                                                     if val is not None]
            metadata["columns"][col_alias]["min"] = [val for val in metadata["columns"][col_alias]["min"]
                                                     if val != float("inf")]

            metadata["columns"][col_alias]["avg"] = round(
                metadata["columns"][col_alias]["total"] / num_value_rows,
                max_precision([metadata["columns"][col_alias]["min"][0], metadata["columns"][col_alias]["max"][0]])
            ) if len(metadata["columns"][col_alias]["min"]) > 0 else None
            metadata["columns"][col_alias].pop("total")


def max_precision(nums):
    """Determine the maximum precision of a list of floating point numbers.

        :param nums: (list(float)) list of numbers
        :return: (int) number of decimal places precision"""
    return max([abs(Decimal(str(num)).as_tuple().exponent) for num in nums])


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
            stripped_field = field.strip()
            if stripped_field != "":
                fields.append(stripped_field)
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
