import logging

from os.path import basename
# noinspection PyPackageRequirements
from alembic.ddl import DefaultImpl
# noinspection PyPackageRequirements
from alembic.ddl.base import AlterColumn, format_table_name, format_column_name, \
    format_type
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import types as sqltypes, Column


from spannerdriver.sqlalchemy.base import SpannerDialect

logger = logging.getLogger('spanner.alembic.%s' %
                           basename(__file__).split('.')[0])


class SpannerAlterColumnType(AlterColumn):

    def __init__(self, name, nullable, column_name, type_, **kw):
        super(SpannerAlterColumnType, self).__init__(name, column_name, **kw)
        self.type_ = sqltypes.to_instance(type_)
        self.nullable = nullable


SUPPORTED_ALTER_TYPE_PAIRS = [
    ('STRING', 'BYTES'),
    ('BYTES', 'STRING')
]


class SpannerAlembicImpl(DefaultImpl):
    __dialect__ = 'spanner'

    transactional_ddl = False

    def alter_supported(self, existing_type, new_type):
        # TODO: Other cases are not supported either (probably)
        if existing_type is None:
            # In some cases we have None as existing type
            return True
        existing_spanner_type = format_type(self, existing_type)
        new_spanner_type = format_type(self, new_type)

        existing_fieldtype, existing_size = SpannerDialect.get_fieldtype_and_size(
            existing_spanner_type)
        new_fieldtype, new_size = SpannerDialect.get_fieldtype_and_size(new_spanner_type)

        if existing_fieldtype == new_fieldtype:
            return True
        else:
            for supported_pair in SUPPORTED_ALTER_TYPE_PAIRS:
                if existing_fieldtype == supported_pair[0] and \
                        new_fieldtype == supported_pair[1]:
                    return True
        return False

    def alter_column(self, table_name, column_name,
                     nullable=None,
                     server_default=False,
                     name=None,
                     type_=None,
                     schema=None,
                     autoincrement=None,
                     existing_type=None,
                     existing_server_default=None,
                     existing_nullable=None,
                     existing_autoincrement=None,
                     **kw
                     ):
        if type_ is not None:
            # HACK: Spanner des not support all ALTER statements - particularly
            # changing type is only allowed BYTES <-> STRING
            if self.alter_supported(existing_type=existing_type, new_type=type_):
                self._exec(SpannerAlterColumnType(
                    name=table_name,
                    nullable=nullable,
                    column_name=column_name,
                    type_=type_,
                    schema=schema,
                    existing_type=existing_type,
                    existing_server_default=existing_server_default,
                    existing_nullable=existing_nullable
                ))
            else:
                logger.warning("Spanner does not support altering column types from "
                               "{} to {}. Emulating it with DROP / ADD but data "
                               "will be lost in table:"
                               " {}, schema: {}, column: {}!".
                               format(existing_type, type_, table_name, schema,
                                      column_name))
                old_column = Column(name=column_name, type_=existing_type,
                                    nullable=existing_nullable)
                self.drop_column(table_name=table_name, column=old_column, schema=schema)
                new_column = Column(name=column_name, type_=type_, nullable=nullable)
                self.add_column(table_name=table_name, column=new_column)

    def rename_table(self, old_table_name, new_table_name, schema=None):
        # HACK: Rename table is not supported. Emulating with DROP/CREATE
        raise NotImplementedError("Spanner does not support renaming tables. "
                                  "We can emulate it in the future with DROP / CREATE "
                                  "but data will be lost from table:"
                                  " {}, renamed to {}!".
                                  format(old_table_name, new_table_name))


# noinspection PyUnusedLocal
@compiles(SpannerAlterColumnType)
def visit_alter_column(element, compiler, **kw):
    table_name = format_table_name(compiler, element.table_name, element.schema)
    column_name = format_column_name(compiler, element.column_name)
    new_type = format_type(compiler, element.type_)

    return "ALTER TABLE {table_name} ALTER COLUMN {column_name} {new_type}".format(
        table_name=table_name, column_name=column_name, new_type=new_type)
