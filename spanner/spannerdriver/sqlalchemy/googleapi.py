import os

from .base import SpannerDialect
from sqlalchemy import pool


# noinspection PyAbstractClass,PyPep8Naming
class SpannerDialect_googleapi(SpannerDialect):
    default_paramstyle = 'pyformat'

    driver = 'googleapi'

    def __init__(self, **kwargs):
        SpannerDialect.__init__(self, **kwargs)

    @classmethod
    def dbapi(cls):
        print(os.environ.get('PYTHONPATH'))
        import spannerdriver.db_api as module
        return module

    @classmethod
    def get_pool_class(cls, url):
        return pool.SingletonThreadPool


dialect = SpannerDialect_googleapi
