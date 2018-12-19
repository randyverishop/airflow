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
from sqlalchemy import sql, schema, types, exc, pool
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base, reflection
from sqlalchemy import processors

# class AcNumeric(types.Numeric):
#     def get_col_spec(self):
#         return "NUMERIC"
#
#     def bind_processor(self, dialect):
#         return processors.to_str
#
#     def result_processor(self, dialect, coltype):
#         return None
#
# class AcFloat(types.Float):
#     def get_col_spec(self):
#         return "FLOAT"
#
#     def bind_processor(self, dialect):
#         """By converting to string, we can use Decimal types round-trip."""
#         return processors.to_str
#
# class AcInteger(types.Integer):
#     def get_col_spec(self):
#         return "INTEGER"
#
# class AcTinyInteger(types.Integer):
#     def get_col_spec(self):
#         return "TINYINT"
#
# class AcSmallInteger(types.SmallInteger):
#     def get_col_spec(self):
#         return "SMALLINT"
#
# class AcDateTime(types.DateTime):
#     def get_col_spec(self):
#         return "DATETIME"
#
# class AcDate(types.Date):
#
#     def get_col_spec(self):
#         return "DATETIME"
#
# class AcText(types.Text):
#     def get_col_spec(self):
#         return "MEMO"
#
# class AcString(types.String):
#     def get_col_spec(self):
#         return "TEXT" + (self.length and ("(%d)" % self.length) or "")
#
# class AcUnicode(types.Unicode):
#     def get_col_spec(self):
#         return "TEXT" + (self.length and ("(%d)" % self.length) or "")
#
#     def bind_processor(self, dialect):
#         return None
#
#     def result_processor(self, dialect, coltype):
#         return None
#
# class AcChar(types.CHAR):
#     def get_col_spec(self):
#         return "TEXT" + (self.length and ("(%d)" % self.length) or "")
#
# class AcBinary(types.LargeBinary):
#     def get_col_spec(self):
#         return "BINARY"
#
# class AcBoolean(types.Boolean):
#     def get_col_spec(self):
#         return "YESNO"
#
# class AcTimeStamp(types.TIMESTAMP):
#     def get_col_spec(self):
#         return "TIMESTAMP"
#

class SpannerExecutionContext(default.DefaultExecutionContext):
    def get_lastrowid(self):
        self.cursor.execute("SELECT @@identity AS lastrowid")
        return self.cursor.fetchone()[0]


class SpannerCompiler(compiler.SQLCompiler):
    extract_map = compiler.SQLCompiler.extract_map.copy()
    # extract_map.update({
    #         'month': 'm',
    #         'day': 'd',
    #         'year': 'yyyy',
    #         'second': 's',
    #         'hour': 'h',
    #         'doy': 'y',
    #         'minute': 'n',
    #         'quarter': 'q',
    #         'dow': 'w',
    #         'week': 'ww'
    # })


class SpannerDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        if column.table is None:
            raise exc.CompileError(
                            "access requires Table-bound columns "
                            "in order to generate DDL")

        colspec = self.preparer.format_column(column)
        seq_col = column.table._autoincrement_column
        if seq_col is column:
            colspec += " AUTOINCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)

            if column.nullable is not None and not column.primary_key:
                if not column.nullable or column.primary_key:
                    colspec += " NOT NULL"
                else:
                    colspec += " NULL"

            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_drop_index(self, drop):
        index = drop.element
        self.append("\nDROP INDEX [%s].[%s]" % \
                        (index.table.name,
                        self._index_identifier(index.name)))


class SpannerIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(['value', 'text'])

    def __init__(self, dialect):
        super(SpannerIdentifierPreparer, self).\
                __init__(dialect, initial_quote='[', final_quote=']')


class SpannerDialect(default.DefaultDialect):
    colspecs = {
        # types.Unicode: AcUnicode,
        # types.Integer: AcInteger,
        # types.SmallInteger: AcSmallInteger,
        # types.Numeric: AcNumeric,
        # types.Float: AcFloat,
        # types.DateTime: AcDateTime,
        # types.Date: AcDate,
        # types.String: AcString,
        # types.LargeBinary: AcBinary,
        # types.Boolean: AcBoolean,
        # types.Text: AcText,
        # types.CHAR: AcChar,
        # types.TIMESTAMP: AcTimeStamp,
    }
    name = 'access'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    poolclass = pool.SingletonThreadPool
    statement_compiler = SpannerCompiler
    ddl_compiler = SpannerDDLCompiler
    preparer = SpannerIdentifierPreparer
    execution_ctx_cls = SpannerExecutionContext

