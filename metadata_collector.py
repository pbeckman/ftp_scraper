import csv
import ftplib


def collect_metadata(ftp, item, directory, extension):
    all_fields = []
    try:
        ftp.retrlines(
            "RETR {}".format(directory + item),
            lambda line: get_fields_from_line(extension, all_fields, line))
    except StandardError:
        pass
    return all_fields


def foo(line):
    print line


def get_fields_from_line(extension, all_fields, line):
    """Add all fields in the given line to `all_fields` if it is a header row.
    Otherwise raise a StandardError so that ftp.retrlines() doesn't continue and read all lines.

                    :param line: (str) file line
                    :param extension: file extension used to determine csv.reader parameters
                    :param all_fields: (list(str)) list in which to store all header fields
                    :raises StandardError: if given line is not a header row"""

    # TODO: determine if whitespace separation for non-csv is effective

    print line

    # choose csv.reader parameters based on file type
    # feeding the csv.reader one [line] splits it into a `row` list of header fields
    if extension == "csv":
        reader = csv.reader([line])
    else:
        reader = csv.reader([line], delimiter=' ', skipinitialspace=True)

    for row in reader:
        # if the row is a header row, add all its fields to the all_fields list
        if is_header_row(row):
            all_fields += row
        # else raise an error that will stop ftp from reading any more lines
        else:
            raise StandardError


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
