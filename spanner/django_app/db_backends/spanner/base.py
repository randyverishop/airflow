from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS
from django.db.backends.base.base import BaseDatabaseWrapper
from .client import SpannerDatabaseClient
from .creation import SpannerDatabaseCreation
from .features import SpannerDatabaseFeatures
from .introspection import SpannerDatabaseIntrospection
from .operations import SpannerDatabaseOperations
from .validation import SpannerDatabaseValidation
from .schema import SpannerDatabaseSchemaEditor

from spanner.spannerdriver.db_api.connection import Connection


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'google'
    display_name = 'Cloud Spanner'
    client_class = SpannerDatabaseClient
    creation_class = SpannerDatabaseCreation
    features_class = SpannerDatabaseFeatures
    introspection_class = SpannerDatabaseIntrospection
    ops_class = SpannerDatabaseOperations
    validation_class = SpannerDatabaseValidation
    SchemaEditorClass = SpannerDatabaseSchemaEditor
    last_ids = {}

    data_types = {
        'AutoField': 'INT64',
        'BigAutoField': 'INT64',
        'BinaryField': 'BYTES',
        'BooleanField': 'BOOL',
        'CharField': 'STRING(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP',
        'DecimalField': 'FLOAT64',
        'DurationField': 'FLOAT64',
        'FileField': 'STRING(%(max_length)s)',
        'FilePathField': 'STRING(%(max_length)s)',
        'FloatField': 'FLOAT64',
        'IntegerField': 'INT64',
        'BigIntegerField': 'INT64',
        'IPAddressField': 'STRING(64)',
        'GenericIPAddressField': 'INT64',
        'NullBooleanField': 'BOOL',
        'OneToOneField': 'INT64',
        'PositiveIntegerField': 'INT64',
        'PositiveSmallIntegerField': 'INT64',
        'SlugField': 'STRING(%(max_length)s)',
        'SmallIntegerField': 'INT64',
        'TextField': 'STRING(MAX)',
        'TimeField': 'STRING(16)',
        'UUIDField': 'STRING(32)',
    }

    # TODO: update it - copy paste from Postgres
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS, allow_thread_sharing=False):
        super().__init__(settings_dict, alias, allow_thread_sharing)

    def create_cursor(self, name=None):
        return self.connection.cursor()

    def get_new_connection(self, conn_params):
        return Connection(
            conn_params['project_id'],
            conn_params['instance_id'],
            conn_params['database_id'],
            credentials_path=conn_params['credentials_json_path']
        )

    def get_connection_params(self):
        settings_dict = self.settings_dict
        for required_variable in ['PROJECT_ID', 'INSTANCE_ID', 'DATABASE_ID', 'CREDENTIALS_JSON_PATH']:
            if required_variable not in settings_dict or not settings_dict.get(required_variable):
                raise ImproperlyConfigured(
                    "settings.DATABASES is improperly configured. "
                    "Please supply the {} value.".format(required_variable))

        conn_params = {
            'database': settings_dict['NAME'] or 'spanner',
            'project_id': settings_dict['PROJECT_ID'],
            'instance_id': settings_dict['INSTANCE_ID'],
            'database_id': settings_dict['DATABASE_ID'],
            'credentials_json_path': settings_dict['CREDENTIALS_JSON_PATH'],
            **settings_dict['OPTIONS'],
        }
        if 'SERVICE_ACCOUNT_JSON' in settings_dict:
            conn_params['service_account_json'] = settings_dict['SERVICE_ACCOUNT_JSON']
        return conn_params

    def _start_transaction_under_autocommit(self):
        pass

    def is_usable(self):
        pass

    def _set_autocommit(self, autocommit):
        pass

    def init_connection_state(self):
        pass


    def set_last_id(self, table_name, id):
        self.last_ids[table_name] = id

    def get_last_id(self, table_name):
        if table_name in self.last_ids:
            last_id = self.last_ids[table_name]
            del self.last_ids[table_name]
            if len(last_id) == 1:
                return last_id[0]
            return last_id
        return None
