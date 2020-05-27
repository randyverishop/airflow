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

# TODO(mik-laj): We have to implement it.
#     Do you want to help? Please look at: https://github.com/apache/airflow/issues/8128
from flask import request

from airflow.api_connexion import parameters
from airflow.api_connexion.schemas.dag_schema import DAGCollection, dags_collection_schema
from airflow.models import DagModel
from airflow.utils.session import provide_session


def get_dag():
    """
    Get basic information about a DAG.
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_dags(session):
    """
    Get all DAGs.
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(request.args.get(parameters.page_limit, 100), 100)
    query = session.query(DagModel)

    query = query.offset(offset).limit(limit)

    dags = query.all()

    return dags_collection_schema.dump(DAGCollection(dag_model=dags, total_entries=0))


def patch_dag():
    """
    Update the specific DAG
    """
    raise NotImplementedError("Not implemented yet.")


def clear_task_instances():
    """
    Clear task instances.
    """
    raise NotImplementedError("Not implemented yet.")
