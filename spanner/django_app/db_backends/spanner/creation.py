from django.db.backends.base.creation import BaseDatabaseCreation


class SpannerDatabaseCreation(BaseDatabaseCreation):
    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        pass
