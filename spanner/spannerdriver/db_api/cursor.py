import collections
import datetime
import logging
import re

# noinspection PyPackageRequirements
import sys
from os.path import basename

# noinspection PyPackageRequirements
from google.cloud.spanner_v1.param_types import STRING, INT64, FLOAT64, BYTES, BOOE, \
    TIMESTAMP, DATE

# noinspection PyPackageRequirements
from google.cloud.spanner_v1.proto import type_pb2

from spannerdriver.db_api import exceptions
# noinspection PyPackageRequirements
from google.api_core.exceptions import InvalidArgument
# noinspection PyPackageRequirements
from spannerdriver.db_api.connection import Connection
logger = logging.getLogger('spanner.db_api.%s' %
                           basename(__file__).split('.')[0])

# Per PEP 249: A 7-item sequence containing information describing one result
# column. The first two items (name and type_code) are mandatory, the other
# five are optional and are set to None if no meaningful values can be
# provided.
Column = collections.namedtuple(
    "Column",
    [
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    ],
)


class SpecialOperation:
    def __init__(self, name, regex, replacement_queries):
        self.name = name
        self.regex = regex
        self.replacement_queries = replacement_queries


# HACK: Spanner does not support certain operations such as updating primary key
#       But there are some valid common cases that we can treat in a special way
# noinspection SqlResolve
SPECIAL_DML_OPERATIONS = [
    SpecialOperation(
        name='Alembic version update',
        regex=re.compile(
            r"UPDATE alembic_version SET version_num='([a-z0-9]+)' "
            r"WHERE alembic_version.version_num = '([a-z0-9]+)'"),
        replacement_queries=[
             "DELETE FROM alembic_version WHERE version_num='{1}'",
             "INSERT INTO alembic_version (version_num) VALUES ('{0}')",
         ]
    )
]


def get_param_type(param):
    if isinstance(param, str):
        return STRING
    if isinstance(param, bool):
        # error in name of Spanner param types :)
        return BOOE
    if isinstance(param, int):
        return INT64
    if isinstance(param, float):
        return FLOAT64
    if isinstance(param, collections.Sequence):
        return BYTES
    if isinstance(param, datetime.date):
        return DATE
    if isinstance(param, datetime.datetime):
        return TIMESTAMP
    if param is None:
        return type_pb2.Type(code=type_pb2.TYPE_CODE_UNSPECIFIED)


class Cursor(object):

    def __init__(self, connection: Connection):
        self.connection = connection
        self.description = None
        # Per PEP 249: The attribute is -1 in case no .execute*() has been
        # performed on the cursor or the rowcount of the last operation
        # cannot be determined by the interface.
        self.rowcount = -1
        self.arraysize = None
        self.result_set = None
        self.query_type = None
        self.snapshot = None
        self.result_array = []
        self.current_result_index = 0
        self.current_result_array_index = 0
        self.session = self.connection.database.session()
        self.session.create()

    def close(self):
        self.session.delete()

    @staticmethod
    def get_operation_type(operation):
        def check_starts_with(commands):
            stripped_operation = operation.strip().upper()
            if isinstance(commands, str):
                commands = [commands]
            for command in commands:
                if stripped_operation.startswith(command):
                    return True
            return False

        if check_starts_with("SELECT"):
            return 'DQL'
        elif check_starts_with(["INSERT", "UPDATE", "DELETE"]):
            return 'DML'
        else:
            return 'DDL'

    def _process_result_set_dql(self, result_set):
        self.result_array = []
        self.current_result_index = 0
        for row in result_set:
            self.result_array.append(row)
        self.rowcount = len(self.result_array)

    def _execute_dql(self, operation, parameters, param_types):
        with self.connection.database.snapshot() as snapshot:
            logger.debug("DQL: %s", operation)
            result_set = snapshot.execute_sql(operation,
                                              params=parameters,
                                              param_types=param_types)
            self._process_result_set_dql(result_set)
            self.description = tuple(
                [
                    Column(
                        name=field.name,
                        type_code=field.type.code,
                        display_size=None,
                        internal_size=None,
                        precision=None,
                        scale=None,
                        null_ok=True,
                    )
                    for field in result_set.fields
                ]
            )
            self.current_result_array_index = 0
            self.rowcount = len(self.result_array)

    def __execute_dml_in_transaction(self, operation,
                                     parameters,
                                     param_types,
                                     transaction):
        logger.debug("DML: %s", operation)
        rowcount = transaction.execute_update(operation,
                                              params=parameters,
                                              param_types=param_types)
        self.result_array = []
        self.current_result_index = 0
        self.rowcount = rowcount

    def _execute_dml(self, operation,
                     parameters,
                     param_types,
                     existing_transaction=None):
        if existing_transaction:
            self.__execute_dml_in_transaction(operation,
                                              parameters,
                                              param_types,
                                              existing_transaction)
        else:
            with self.session.transaction() as new_transaction:
                self.__execute_dml_in_transaction(operation,
                                                  parameters,
                                                  param_types,
                                                  new_transaction)

    def _execute_dml_with_special_handling(self, operation, parameters, param_types):
        for special_operation in SPECIAL_DML_OPERATIONS:
            match = special_operation.regex.match(operation)
            if match:
                logger.info("Executing special handling for %s",
                            special_operation.name)
                last_result = None
                with self.session.transaction() as transaction:
                    for query in special_operation.replacement_queries:
                        self._execute_dml(
                            query.format(*match.groups()),
                            parameters=parameters,
                            param_types=param_types,
                            existing_transaction=transaction)

                last_result
            else:
                self._execute_dml(operation,
                                  parameters=parameters,
                                  param_types=param_types)

    def _execute_ddl(self, operation):
        logger.debug("DDL: %s", operation)
        spanner_op = self.connection.database.update_ddl([operation])
        if spanner_op:
            result = spanner_op.result()
            logger.info(result)

    def _run_operation(self, operation, parameters=None, param_types=None):
        operation_type = self.get_operation_type(operation,)
        if operation_type == 'DQL':
            self._execute_dql(operation, parameters, param_types)
        if operation_type == 'DML':
            self._execute_dml_with_special_handling(operation, parameters, param_types)
        elif operation_type == 'DDL':
            self._execute_ddl(operation)

    # noinspection PyUnusedLocal
    def execute(self, operation, parameters=None, job_id=None):
        try:
            spanner_param_types = {}
            spanner_param_values = {}
            if parameters and len(parameters.keys()) > 0:
                logger.debug("Preparing parameter query:'%s' with parameters:'%s",
                             operation, parameters)
                spanner_param_names = {}
                # Convert parameters into @-style parameters of spanner
                # this helps us to avoid quoting parameters - we can still
                # pass @-style parameters and empty string and it will work fine.
                # If you try to prepare the query using '%' formatting, the
                # quoting of strings is missing
                for key in parameters.keys():
                    spanner_param_names[key] = '@' + key
                    spanner_param_types[key] = \
                        get_param_type(parameters[key])
                    spanner_param_values[key] = parameters[key]
                operation = operation % spanner_param_names
                logger.info("Running query after preparing parameters: '%s' "
                            "params: %s, param types: %s",
                            operation, spanner_param_values, spanner_param_types)
            self._run_operation(operation, parameters=spanner_param_values,
                                param_types=spanner_param_types)
        except InvalidArgument as e:
            logger.warning("Exception when running query: %s", e)
            raise exceptions.NotSupportedError

    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        if self.result_array is None:
            raise exceptions.InterfaceError(
                "No query results: execute() must be called before fetch."
            )
        if self.current_result_index >= len(self.result_array):
            return None
        else:
            res = self.result_array[self.current_result_index]
            self.current_result_index += 1
            return res

    def fetchmany(self, size=None):
        if self.result_array is None:
            raise exceptions.InterfaceError(
                "No query results: execute() must be called before fetch."
            )
        if not size:
            size = self.arraysize
        if size is None:
            size = sys.maxsize
        res = []
        while self.current_result_index < len(self.result_array) and len(res) < size:
            res.append(self.fetchone())
        return res

    def fetchall(self):
        return self.fetchmany(size=sys.maxsize)

    def setinputsizes(self, sizes):
        """No-op."""

    def setoutputsize(self, size, column=None):
        """No-op."""
