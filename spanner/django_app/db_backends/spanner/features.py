from django.db.backends.base.features import BaseDatabaseFeatures


class SpannerDatabaseFeatures(BaseDatabaseFeatures):
    supports_foreign_keys = False
    implied_column_null = True

    # HACK Spanner supports bulk inserts, but due to missing auto-fields in Spanner,
    # IDs must be set locally and bulk inserts are no-go for now
    has_bulk_insert = False

    supports_paramstyle_pyformat = False
    can_return_id_from_insert = False
    can_return_ids_from_bulk_insert = False
