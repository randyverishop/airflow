__version__ = '0.8'

from sqlalchemy.dialects import registry

registry.register("spanner", "sqlalchemy_spanner.googleapi", "SpannerDialect_googleapi")
