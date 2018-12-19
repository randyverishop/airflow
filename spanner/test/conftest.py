from sqlalchemy.dialects import registry

registry.register("spanner", "sqlalchemy_spanner.googleapi", "SpannerDialect_googleapi")


from sqlalchemy.testing.plugin.pytestplugin import *
