import unittest

from airflow.api_connexion.schemas.dag_schema import DAGSchema
from airflow.models import DagModel, DagTag
from airflow.utils.session import provide_session
from test_utils.db import clear_db_dags


class TestDagSchema(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_dags()

    def tearDown(self) -> None:
        clear_db_dags()

    @provide_session
    def test_serialzie(self, session):
        dag_model = DagModel(
            dag_id="test_dag_id",
            root_dag_id="test_root_dag_id",
            is_paused=True,
            is_subdag=False,
            fileloc="/root/airflow/dags/my_dag.py",
            owners="airflow1,airflow2",
            description="The description",
            schedule_interval="5 4 * * *",
            tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
        )
        session.add(dag_model)
        session.commit()
        dag_model = session.query(DagModel).first()
        deserialized_dag = DAGSchema().dump(dag_model)
        self.assertEqual(
            deserialized_dag,
            {
                "dag_id": "test_dag_id",
                "description": "The description",
                "fileloc": "/root/airflow/dags/my_dag.py",
                "is_paused": True,
                "is_subdag": False,
                'owners': ['airflow1', 'airflow2'],
                "root_dag_id": "test_root_dag_id",
                'schedule_interval': {'__type': 'cron_expression', 'value': '5 4 * * *'},
                "tags": [{"name": "tag-1"}, {"name": "tag-2"}],
            },
        )

