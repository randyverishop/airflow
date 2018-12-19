# noinspection PyProtectedMember
import logging

# noinspection PyProtectedMember
from sqlalchemy.dialects import registry
# Make sure Spanner Alembic Implementation registers itself with Alembic
# noinspection PyPackageRequirements
from spannerdriver.alembic.spanner_alembic_impl import SpannerAlembicImpl

SpannerAlembicImpl.__dialect__

registry.register("spanner", "spannerdriver.sqlalchemy.googleapi",
                  "SpannerDialect_googleapi")
logger = logging.getLogger('spanner')
logger.setLevel(logging.DEBUG)

logger.info("#" * 80)
logger.info("Spanner Driver initialized !!!")
logger.info("#" * 80)
