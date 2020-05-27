from typing import List, NamedTuple

from marshmallow import fields, Schema
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.common_schema import ScheduleIntervalSchema
from airflow.models.dag import DagModel, DagTag


class DagTagSchema(SQLAlchemySchema):
    class Meta:
        model = DagTag

    name = auto_field()


class DAGSchema(SQLAlchemySchema):
    class Meta:
        model = DagModel

    dag_id = auto_field(dump_only=True)
    root_dag_id = auto_field(dump_only=True)
    is_paused = auto_field()
    is_subdag = auto_field(dump_only=True)
    fileloc = auto_field(dump_only=True)
    owners = fields.Method("get_owners", dump_only=True)
    description = auto_field(dump_only=True)
    schedule_interval = fields.Nested(ScheduleIntervalSchema, dump_only=True)
    tags = fields.List(fields.Nested(DagTagSchema), dump_only=True)

    @staticmethod
    def get_owners(obj: DagModel):
        if not obj.owners:
            return []
        return obj.owners.split(",")


class DAGCollection(NamedTuple):
    dags: List[DagModel]
    total_entries: int


class DAGCollectionSchema(Schema):
    dag_model = fields.List(fields.Nested(DAGSchema))
    total_entries = fields.Int()


dags_collection_schema = DAGCollectionSchema()
