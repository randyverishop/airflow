__version__ = '0.8'

# noinspection PyProtectedMember
from sqlalchemy.dialects import registry

registry.register("spanner", "spanner.sqlalchemy.googleapi",
                  "SpannerDialect_googleapi")


__all__ = [
    'base', 'googleapi', 'requirements'
]
