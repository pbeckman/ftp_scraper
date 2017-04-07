import csv
import json
import numpy
import os
import re
from netCDF4 import Dataset
from decimal import Decimal
from operator import itemgetter


# TODO: bounding box method for lat and lon lists?
# TODO: granularity of data?


class ExtractionError(Exception):
    """Basic error to throw when an extractor fails"""


def extract_metadata(file_name, path):
    """Create metadata JSON from file.

        :param file_name: (str) file name
        :param path: (str) absolute or relative path to file
        :returns: (dict) metadata dictionary"""

    with open(path + file_name, 'rU') as file_handle:

        extension = file_name.split('.', 1)[1] if '.' in file_name else "no extension"

        metadata = {}

        try:
            metadata = extract_columnar_metadata(file_handle)
        except ExtractionError:
            # not a columnar file
            pass

        if extension == "nc":
            metadata = extract_netcdf_metadata(file_handle)

    return metadata


def extract_netcdf_metadata(file_handle):
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


def extract_columnar_metadata(file_handle):
    """Get metadata from column-formatted file.

        :param file_handle: (file) open file
        :returns: (dict) ascertained metadata
        :raises: (ExtractionError) if the file cannot be read as a columnar file"""

    extension = file_handle.name.split('.', 1)[1] if '.' in file_handle.name else "no extension"

    # choose csv.reader parameters based on file type - if not csv, use whitespace-delimited
    reverse_reader = ReverseReader(file_handle, delimiter="," if extension in ["csv", "exc.csv"] else "whitespace")

    # base dictionary in which to store all the metadata
    metadata = {"columns": {}}

    # minimum number of rows to be considered an extractable table
    min_rows = 3
    # size of extracted free-text preamble in characters
    preamble_size = 1000

    headers = []
    col_types = []
    col_aliases = []
    num_value_rows = 0
    num_header_rows = 0
    # used to check if all rows are the same length, if not, this is not a valid columnar file
    row_length = 0
    is_first_row = True
    fully_parsed = True

    # save the last l rows to try to parse them later
    last_rows = [reverse_reader.next() for i in range(0, 3)]

    # now we try to extract a table from the remaining n-l rows
    for row in reverse_reader:
        # if row is not the same length as previous row, raise an error showing this is not a valid columnar file
        if not is_first_row and row_length != len(row):
            # tables are not worth extracting if under this row threshold
            if num_value_rows < min_rows:
                raise ExtractionError
            else:
                fully_parsed = False
                break
        # update row length for next check
        row_length = len(row)

        if is_first_row:
            # make column aliases so that we can create aggregates even for unlabelled columns
            col_aliases = ["__{}__".format(i) for i in range(0, row_length)]
        is_first_row = False

        # if the row is a header row, add all its fields to the headers list
        if is_header_row(row):
            # tables are likely not representative of the file if under this row threshold, so so not extract metadata
            if num_value_rows < min_rows:
                raise ExtractionError
            # set the column aliases to the most recent header row if they are unique
            if len(set(row)) == len(row):
                for i in range(0, len(row)):
                    metadata["columns"][row[i]] = metadata["columns"].pop(col_aliases[i])
                col_aliases = row

            num_header_rows += 1
            for header in row:
                if header != "":
                    headers.append(header)

        else:
            num_value_rows += 1

            # type check the first row to decide which aggregates to use
            if num_value_rows == 1:
                col_types = ["num" if is_number(field) else "str" for field in row]

            add_row_to_aggregates(metadata, row, col_aliases, col_types, num_value_rows == 1)

    # add the originally skipped rows into the aggregates
    for row in last_rows:
        if len(row) == row_length:
            add_row_to_aggregates(metadata, row, col_aliases, col_types, num_value_rows == 1)

    # number of characters in file before last un-parse-able row
    if not fully_parsed:
        file_handle.seek(reverse_reader.prev_position)
        remaining_chars = file_handle.tell() - 1
        # extract free-text preamble, which may contain headers
        if remaining_chars >= preamble_size:
            file_handle.seek(-preamble_size, 1)
        else:
            file_handle.seek(0)
        preamble = ""
        # do this instead of passing an argument to read() to avoid multi-byte character encoding difficulties
        while file_handle.tell() <= reverse_reader.prev_position:
            preamble += file_handle.read(1)
        # add preamble to the metadata if the whole file hasn't already been processed
        if len(preamble) > 0:
            metadata["preamble"] = preamble

    # add header list to metadata
    if len(headers) > 0:
        metadata["headers"] = list(set(headers))

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
            # the try except is used to pass over textual and black space nulls on which type coercion will fail
            try:
                value = float(value)
            except ValueError:
                continue

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


class ReverseReader:
    """Reads column-formatted files in reverse as lists of fields.

        :param file_handle: (file) open file
        :param delimiter: (string) ',' or 'whitespace' """

    def __init__(self, file_handle, delimiter=","):
        self.fh = file_handle
        self.fh.seek(0, os.SEEK_END)
        self.delimiter = delimiter
        self.position = self.fh.tell()
        self.prev_position = self.fh.tell()

    @staticmethod
    def fields(line, delimiter):
        # if space-delimited, do not keep whitespace fields, otherwise do
        return [field.strip() for field in re.split("," if delimiter == "," else "\\s", line)
                if delimiter != "whitespace" or delimiter == "whitespace" and field.strip() != ""]

    def next(self):
        line = ''
        if self.position <= 0:
            raise StopIteration
        self.prev_position = self.position
        while self.position >= 0:
            self.fh.seek(self.position)
            next_char = self.fh.read(1)
            if next_char in ['\n', '\r']:
                self.position -= 1
                if len(line) > 1:
                    return self.fields(line[::-1], self.delimiter)
            else:
                line += next_char
                self.position -= 1
        return self.fields(line[::-1], self.delimiter)

    def __iter__(self):
        return self


def is_header_row(row):
    """Determine if row is a header row by checking that it contains no fields that are
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
