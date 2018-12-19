#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Support for the Spanner database.


"""
import logging
import re
from os.path import basename

from sqlalchemy import pool, util, ColumnDefault
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import compiler, sqltypes
from sqlalchemy.engine import default

logger = logging.getLogger('spanner.sqlalchemy.%s' %
                           basename(__file__).split('.')[0])

ischema_names = {
    'ARRAY': sqltypes.ARRAY,
    'BOOL': sqltypes.BOOLEAN,
    'BYTES': sqltypes.BLOB,
    'DARE': sqltypes.DATE,
    'FLOAT64': sqltypes.FLOAT,
    'INT64': sqltypes.BIGINT,
    'STRING': sqltypes.VARCHAR,
    'STRUCT': None,
    'TIMESTAMP': sqltypes.TIMESTAMP,
}


class SpannerExecutionContext(default.DefaultExecutionContext):
    def create_server_side_cursor(self):
        raise NotImplementedError()

    def result(self):
        raise NotImplementedError()

    def get_rowcount(self):
        raise NotImplementedError()

    def get_lastrowid(self):
        return None


class SpannerCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence):
        raise NotImplementedError()

    extract_map = compiler.SQLCompiler.extract_map.copy()


class SpannerDDLCompiler(compiler.DDLCompiler):

    # noinspection PyDefaultArgument
    def __init__(self, dialect, statement, bind=None,
                 schema_translate_map=None,
                 compile_kwargs=util.immutabledict()):
        super(SpannerDDLCompiler, self).__init__(dialect, statement, bind,
                                                 schema_translate_map,
                                                 compile_kwargs)
        self.primary_key_clause = None

    def visit_create_table(self, create):
        return super(SpannerDDLCompiler, self).visit_create_table(create)

    def get_column_specification(self, column, **kwargs):
        coltype = self.dialect.type_compiler.process(
            column.type, type_expression=column)
        colspec = self.preparer.format_column(column) + " " + coltype
        return colspec

    def visit_create_column(self, create, first_pk=False):
        return super(SpannerDDLCompiler, self).visit_create_column(create, first_pk)

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return ''
        text = ""
        text += " PRIMARY KEY "
        # noinspection PyProtectedMember
        text += "(%s)" % ', '.join(
            self.preparer.quote(c.name)

            for c in (constraint.columns_autoinc_first
                      if constraint._implicit_generated
                      else constraint.columns))
        self.primary_key_clause = text
        return ''

    def visit_unique_constraint(self, constraint):
        logger.warning("Unique constraint is not yet implemented %s", constraint)
        return None

    def visit_foreign_key_constraint(self, constraint):
        logger.warning("Foreign key constraints are not implemented %s", constraint)
        return None

    def post_create_table(self, table):
        if self.primary_key_clause:
            return self.primary_key_clause
        else:
            return ''


# noinspection PyUnusedLocal
@compiles(ColumnDefault, 'spanner')
def visit_column_default(element, compiler_, **kw):
    raise NotImplementedError()


class SpannerTypeCompiler(compiler.GenericTypeCompiler):
    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "STRING")

    def visit_INTEGER(self, type_, **kw):
        return 'INT64'

    def visit_BIGINT(self, type_, **kw):
        return 'INT64'

    def visit_BOOLEAN(self, type_, **kw):
        return 'BOOL'

    def visit_DATETIME(self, type_, **kw):
        return 'TIMESTAMP'

    def visit_BLOB(self, type_, **kw):
        return 'BYTES(MAX)'

    def visit_TEXT(self, type_, **kw):
        return 'STRING(MAX)'

    def visit_FLOAT(self, type_, **kw):
        return 'FLOAT64'

    def visit_large_binary(self, type_, **kw):
        return 'BYTES(MAX)'


class SpannerIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.discard('user')

    def __init__(self, dialect):
        super(SpannerIdentifierPreparer, self).\
                __init__(dialect, initial_quote="'", final_quote="'")


# noinspection PyAbstractClass,SqlResolve
class SpannerDialect(default.DefaultDialect):
    name = 'spanner'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    ischema_names = ischema_names
    poolclass = pool.SingletonThreadPool
    statement_compiler = SpannerCompiler
    ddl_compiler = SpannerDDLCompiler
    preparer = SpannerIdentifierPreparer
    execution_ctx_cls = SpannerExecutionContext
    type_compiler = SpannerTypeCompiler
    supports_native_boolean = True

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("""
SELECT
    t.table_name
FROM
    information_schema.tables AS t
WHERE
    t.table_name = '{}'
""".format(table_name, schema))
        return bool(cursor.first())

    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute("""
SELECT
    t.table_name
FROM
    information_schema.tables AS t
""".format(schema)
        )
        return [name for name, in result]

    @staticmethod
    def get_fieldtype_and_size(spanner_type):
        res = re.match(r'^([A-Z0-9]*)(\([0-9]*\))?$', spanner_type)
        if not res:
            return None, None
        groups = res.groups()
        fieldtype = ischema_names.get(groups[0])
        if not groups[1] == 1:
            return fieldtype, None
        else:
            size = groups[1][1:-1]
            if size == 'MAX':
                num_size = 10*1024*1024  # 10 MB is max size
            else:
                num_size = int(size)
            return fieldtype, num_size

    def get_columns(self, connection, table_name, schema=None, **kw):
        primary_keys_result = connection.execute("""
SELECT 
    i.COLUMN_NAME
    FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS i
    WHERE i.TABLE_NAME = '{}' AND i.INDEX_TYPE = 'PRIMARY_KEY'
""".format(table_name))
        primary_keys = [column_name for column_name in primary_keys_result]
        result = connection.execute("""
SELECT 
    i.COLUMN_NAME, i.SPANNER_TYPE, i.IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS AS i
    WHERE i.TABLE_NAME = '{}'
""".format(table_name))
        cols_info = []
        for (column_name, spanner_type, is_nullable) in result:
            fieldtype, the_size = self.get_fieldtype_and_size(spanner_type)
            cols_info.append({
                'name': column_name,
                'type': fieldtype,
                'nullable': is_nullable,
                'default': None,
                'autoincrement': False,
                'primary_key': column_name in primary_keys
            })
        return cols_info
