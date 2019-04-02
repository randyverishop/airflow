from django.db.backends.base.client import BaseDatabaseClient


class SpannerDatabaseClient(BaseDatabaseClient):
    def runshell(self):
        print("Interactive shell isn't provided by Google")
        print("Possibly https://github.com/yfuruyama/spanner-cli can be used")
        return
