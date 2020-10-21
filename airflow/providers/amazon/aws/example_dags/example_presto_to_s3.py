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

# example_presto_to_s3.py
from airflow import models
from airflow.providers.amazon.aws.transfers.presto_to_s3 import PrestoToS3Operator
from airflow.utils.dates import days_ago


with models.DAG(
    dag_id="presto_to_s3_example2",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(10),
    tags=["example"],
) as dag:
    presto_to_s3 = PrestoToS3Operator(
        presto_conn_id="presto_default",
        task_id="presto_to_s3",
        sql="SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3",
        filename=f"/tmp/test.csv",
        bucket='test-from-airflow-to-wix-with-love',
    )
