from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils.functional import cached_property


class SpannerDatabaseOperations(BaseDatabaseOperations):
    def time_trunc_sql(self, lookup_type, field_name):
        print('time_trunc_sql')
        pass

    def quote_name(self, name):
        return name

    def date_interval_sql(self, timedelta):
        print('date_interval_sql')
        pass

    def no_limit_value(self):
        print('no_limit_value')
        pass

    def date_extract_sql(self, lookup_type, field_name):
        print('date_extract_sql')
        pass

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        print('sql_flush')
        pass

    def regex_lookup(self, lookup_type):
        print('regex_lookup')
        pass

    def datetime_cast_date_sql(self, field_name, tzname):
        print('datetime_cast_date_sql')
        pass

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        print('datetime_trunc_sql')
        pass

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        print('datetime_extract_sql')
        pass

    def date_trunc_sql(self, lookup_type, field_name):
        print('date_trunc_sql')
        pass


    def datetime_cast_time_sql(self, field_name, tzname):
        print('datetime_cast_time_sql')
        pass

    def last_insert_id(self, cursor, table_name, pk_name):
        return self.connection.get_last_id(table_name)

    @cached_property
    def compiler_module(self):
        return 'db_backends.spanner.compiler'
