from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo


class SpannerDatabaseIntrospection(BaseDatabaseIntrospection):
    def get_key_columns(self, cursor, table_name):
        print("get_key_columns")
        return []

    def get_sequences(self, cursor, table_name, table_fields=()):
        print("get_sequences")
        return []

    def get_constraints(self, cursor, table_name):
        print("get_constraints")
        return []

    def get_table_list(self, cursor):
        cursor.execute("SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''")
        res = cursor.fetchall()
        x= [
            TableInfo(row[0], 't') for row in res
        ]
        return x

