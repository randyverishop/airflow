class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        # Per PEP 249: The attribute is -1 in case no .execute*() has been
        # performed on the cursor or the rowcount of the last operation
        # cannot be determined by the interface.
        self.rowcount = -1
        # Per PEP 249: The arraysize attribute defaults to 1, meaning to fetch
        # a single row at a time.
        self.arraysize = 1
        self._query_data = None
        self._query_job = None

    def close(self):
        """No-op."""

    def execute(self, operation, parameters=None, job_id=None):
        print("Executing {} {} {}".format(operation, parameters, job_id))
        pass

    def executemany(self, operation, seq_of_parameters):
        """Prepare and execute a database operation multiple times.
        :type operation: str
        :param operation: A Google BigQuery query string.
        :type seq_of_parameters: Sequence[Mapping[str, Any] or Sequence[Any]]
        :param parameters: Sequence of many sets of parameter values.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        return [""]

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    def setinputsizes(self, sizes):
        """No-op."""

    def setoutputsize(self, size, column=None):
        """No-op."""
