from .connection import connect
from .connection import Connection
from .cursor import Cursor
from .exceptions import Error, Warning, InterfaceError, DatabaseError, DataError, \
    OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError

apilevel = "2.0"

threadsafety = 0

paramstyle = "pyformat"

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
