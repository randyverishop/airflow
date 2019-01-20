"""Types used in the DB-API.

See `PEP-249`_ for details.

.. _PEP-249:
    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
"""
import base64
import datetime


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def Binary(string):
    """Construct a DB-API binary value.

    :type string: str
    :param string: A string to encode as a binary value.

    :rtype: bytes
    :returns: The UTF-8 encoded bytes representing the string.
    """
    # Note in Python 3 we got bytes here already - no need to convert it
    if isinstance(string, bytes):
        return base64.encodebytes(string)
    return string.encode("utf-8")


def TimeFromTicks(ticks, tz=None):
    """Construct a DB-API time value from the given ticks value.

    :type ticks: float
    :param ticks:
        a number of seconds since the epoch; see the documentation of the
        standard Python time module for details.

    :type tz: :class:`datetime.tzinfo`
    :param tz: (Optional) time zone to use for conversion

    :rtype: :class:`datetime.time`
    :returns: time represented by ticks.
    """
    dt = datetime.datetime.fromtimestamp(ticks, tz=tz)
    return dt.timetz()


class _DBAPITypeObject(object):
    """DB-API type object which compares equal to many different strings.

    See `PEP-249`_ for details.

    .. _PEP-249:
        https://www.python.org/dev/peps/pep-0249/#implementation-hints-for-module-authors
    """

    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


STRING = "STRING"
BINARY = _DBAPITypeObject("BYTES", "RECORD", "STRUCT")
NUMBER = _DBAPITypeObject(
    "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BOOLEAN", "BOOL"
)
DATETIME = _DBAPITypeObject("TIMESTAMP", "DATE", "TIME", "DATETIME")
ROWID = "ROWID"
