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
#     Do you want to help? Please look at: https://github.com/apache/airflow/issues/8131
from flask import request

from airflow.api_connexion import parameters
from airflow.api_connexion.schemas.pool_schema import PoolCollection, pool_collection_schema, pool_schema
from airflow.models import Pool
from airflow.utils.session import provide_session


def delete_pool():
    """
    Delete a pool
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_pool(session, pool_name):
    """
    Get a pool
    """
    query = session.query(Pool)

    query = query.filter(Pool.pool == pool_name)

    pool = query.one_or_none()

    return pool_schema.dump(pool)


@provide_session
def get_pools(session):
    """
    Get all pools
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(request.args.get(parameters.page_limit, 100), 100)
    query = session.query(Pool)

    query = query.offset(offset).limit(limit)

    pools = query.all()

    total_entries = session.query(Pool).count()

    return pool_collection_schema.dump(PoolCollection(pools=pools, total_entries=total_entries))


def patch_pool():
    """
    Update a pool
    """
    raise NotImplementedError("Not implemented yet.")


def post_pool():
    """
    Create aa pool
    """
    raise NotImplementedError("Not implemented yet.")
