from sqlalchemy.dialects import registry

registry.register("spanner",
                  "spannerdriver.sqlalchemy.googleapi",
                  "SpannerDialect_googleapi")

from sqlalchemy.testing import runner

runner.main()
