#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG that showss interactions with Google Cloud Firestore.
"""

import os

from future.backports.urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import CloudBuildCreateOperator
from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

GCP_EXPORT_DESTINATION_URL = os.environ.get("G_PROJECT_ARCHIVE_URL", "gs://example-bucket/namespace/")


with models.DAG(
    "example_google_firestore",
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_export_database_to_gcs]
    create_build_from_storage = CloudFirestoreExportDatabaseOperator(
        task_id="export_database_to_gcs",
        project_id=GCP_PROJECT_ID,
        # database_id="AAA",
        body={
            "outputUriPrefix": GCP_EXPORT_DESTINATION_URL
        }
    )
    # [END howto_operator_export_database_to_gcs]
