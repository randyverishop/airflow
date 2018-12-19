from .cursor import Cursor


class Connection(object):
    """DB-API Connection to Cloud Spanner.
    :type client: :class:`~cloud.spanner.Client`
    :param client: A client used to connect to Spanner.
    """

    def __init__(self, **kwargs):
        pass

    def close(self):
        """No-op."""

    def commit(self):
        """No-op."""

    def rollback(self):
        """No-op."""

    def cursor(self):
        """Return a new cursor object.
        :rtype: :class:`~google.cloud.bigquery.dbapi.Cursor`
        :returns: A DB-API cursor that uses this connection.
        """
        return Cursor(self)


def connect(**kwargs):
    return Connection(**kwargs)
