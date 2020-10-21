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

import csv
import os
from typing import Any
from uuid import uuid4

from prestodb.dbapi import Cursor as PrestoCursor

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.utils.decorators import apply_defaults


class _PrestoToS3PrestoCursorAdapter:
    """
    An adapter that adds iterator interface.

    A detailed description of the class methods is available in
    `PEP-249 <https://www.python.org/dev/peps/pep-0249/>`__.
    """

    def __init__(self, cursor: PrestoCursor):
        self.cursor: PrestoCursor = cursor

    def __next__(self) -> Any:
        """
        Return the next row from the currently executing SQL statement using the same semantics as
        ``.fetchone()``.  A ``StopIteration`` exception is raised when the result set is exhausted.
        :return:
        """
        result = self.cursor.fetchone()
        if result is None:
            raise StopIteration()
        return result

    def __iter__(self) -> "_PrestoToS3PrestoCursorAdapter":
        """Return self to make cursors compatible to the iteration protocol"""
        return self


class PrestoToS3Operator(BaseOperator):
    """Copy data from PrestoDB to S3 in JSON format.

    :param presto_conn_id: Reference to a specific Presto hook.
    :type presto_conn_id: str
    """

    @apply_defaults
    def __init__(
        self, *, sql: str, presto_conn_id: str = "presto_default", bucket: str, filename: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.presto_conn_id = presto_conn_id
        self.sql = sql
        self.filename = filename
        self.bucket = bucket

    def _write_local_data_file(self, cursor) -> str:
        """
        Takes a cursor, and writes results to a local csv file.

        """
        with open(self.filename, mode="wt") as file_handle:
            csv_writer = csv.writer(file_handle)
            for row in cursor:
                csv_writer.writerow(row)

    def _upload_to_s3(self, filename):
        s3_client = S3Hook().get_conn()
        s3_client.upload_file(
            Filename=self.filename,
            Bucket=self.bucket,
            Key="test" + str(uuid4()),
        )

    def execute(self, context):
        self.log.info("Executing query")
        cursor = self.query()

        self._write_local_data_file(cursor)
        self.log.info("Writing local data files")

        self.log.info("Uploading files to S3: %s", self.filename)
        self._upload_to_s3(self.filename)

        self.log.info("Removing local file")
        os.remove(self.filename)

    def query(self):
        """Queries presto and returns a cursor to the results."""
        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        conn = presto.get_conn()
        cursor = conn.cursor()
        self.log.info("Executing: %s", self.sql)
        cursor.execute(self.sql)
        return _PrestoToS3PrestoCursorAdapter(cursor)
