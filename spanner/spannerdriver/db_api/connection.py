# noinspection PyPackageRequirements
from google.cloud.spanner_v1 import Client
# noinspection PyPackageRequirements
import google.auth

_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


class Connection(object):
    """DB-API Connection to Cloud Spanner.
    """

    def __init__(self, project_id, instance_id, database_id):
        pass
        # noinspection PyUnresolvedReferences
        credentials, _ = google.auth.default(scopes=_DEFAULT_SCOPES)

        self.client = Client(project=project_id, credentials=credentials)
        self.instance = self.client.instance(instance_id=instance_id)
        self.database = self.instance.database(database_id=database_id)

    def close(self):
        """No-op."""

    def commit(self):
        """No-op."""

    def rollback(self):
        """No-op."""

    def cursor(self):
        from .cursor import Cursor
        return Cursor(self)


def connect(**kwargs):
    connection = Connection(**kwargs)
    return connection
