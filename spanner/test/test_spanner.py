import os
import unittest
from filecmp import cmp
from os.path import expanduser
from shutil import copy


class SpannerInitDBTest(unittest.TestCase):

    def setUp(self):
        if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            raise Exception("Please set GOOGLE_APPLICATION_CREDENTIALS environment "
                            "variable to point to service account key with SpannerAdmin"
                            "role before running the test.")
        airflow_spanner_config_file = os.path.realpath(
            os.path.join(os.path.dirname(__file__),
                         os.pardir,
                         "airflow_spanner.cfg"))
        if os.environ.get("AIRFLOW_CONFIG") != airflow_spanner_config_file:
            raise Exception("Please set AIRFLOW_CONFIG environment variable to {}".
                            format(airflow_spanner_config_file))
        airflow_home = os.environ.get("airflow_home", expanduser("~"))
        airflow_plugin_dir = os.path.join(airflow_home, "airflow", "plugin")
        if not os.path.exists(airflow_plugin_dir):
            os.makedirs(airflow_plugin_dir)
        if not os.path.isdir(airflow_plugin_dir):
            raise Exception("The plugin dir is not a dir: {}".format(airflow_plugin_dir))
        airflow_spanner_source_plugin_file = os.path.realpath(
            os.path.join(os.path.dirname(__file__),
                         os.pardir,
                         "spanner_airflow_plugin.py"))
        airflow_spanner_target_plugin_file = os.path.join(airflow_plugin_dir,
                                                          "spanner_airflow_plugin.py")
        if not os.path.exists(airflow_spanner_target_plugin_file):
            copy(airflow_spanner_source_plugin_file, airflow_spanner_target_plugin_file)
            raise Exception(
                "Please re-run the test. The airflow plugin was copied from: {} to {} ".
                format(
                    airflow_spanner_source_plugin_file,
                    airflow_spanner_target_plugin_file))
        if not cmp(airflow_spanner_target_plugin_file,
                   airflow_spanner_source_plugin_file):
            copy(airflow_spanner_source_plugin_file, airflow_spanner_source_plugin_file)
            raise Exception(
                "Please re-run the test. The airflow plugin has changed and was copied "
                "from: {} to {} ".format(
                    airflow_spanner_source_plugin_file,
                    airflow_spanner_target_plugin_file))

    # noinspection PyMethodMayBeStatic
    def test_initdb(self):
        from airflow import settings
        from airflow.utils import db as db_utils

        db_utils.initdb(settings.RBAC)

