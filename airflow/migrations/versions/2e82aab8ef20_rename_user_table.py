# -*- coding: utf-8 -*-
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

"""rename user table

Revision ID: 2e82aab8ef20
Revises: 1968acfc09e3
Create Date: 2016-04-02 19:28:15.211915

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
from sqlalchemy.engine.reflection import Inspector

revision = '2e82aab8ef20'
down_revision = '1968acfc09e3'
branch_labels = None
depends_on = None


def upgrade():
    # op.rename_table('user', 'users')

    # HACK: instead of implementing rename for table it is faster to
    # Recreate the table for the POC. This recreate is merge from
    # the past migrations!
    conn = op.get_bind()

    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if 'user' in tables:
        op.drop_table('user')

    if 'users' not in tables:
        op.create_table(
            'users',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('username', sa.String(length=250), nullable=True),
            sa.Column('email', sa.String(length=500), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username'),
            sa.Column('password', sa.String(255))
        )


def downgrade():
    # op.rename_table('users', 'user')
    conn = op.get_bind()

    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if 'users' in tables:
        op.drop_table('users')

    if 'user' not in tables:
        op.create_table(
            'user',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('username', sa.String(length=250), nullable=True),
            sa.Column('email', sa.String(length=500), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username'),
            sa.Column('password', sa.String(255))
        )
