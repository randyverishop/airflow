import datetime
import unittest

from dateutil import relativedelta

from airflow.api_connexion.schemas.common_schema import (
    TimeDeltaSchema,
    RelativeDeltaSchema,
    CronExpressionSchema,
    ScheduleIntervalSchema,
    CronExpression,
)


class TestTimeDeltaSchema(unittest.TestCase):
    def test_should_serialize(self):
        instance = datetime.timedelta(days=12)
        schema_instance = TimeDeltaSchema()
        result = schema_instance.dump(instance)
        self.assertEqual({"__type": "time_delta", "days": 12, "seconds": 0}, result)

    def test_should_deserialize(self):
        instance = {"__type": "time_delta", "days": 12, "seconds": 0}
        schema_instance = TimeDeltaSchema()
        result = schema_instance.load(instance)
        expected_instance = datetime.timedelta(days=12)
        self.assertEqual(expected_instance, result)


class TestRelativeDeltaSchema(unittest.TestCase):
    def test_should_serialize(self):
        instance = relativedelta.relativedelta(days=+12)
        schema_instance = RelativeDeltaSchema()
        result = schema_instance.dump(instance)
        self.assertEqual(
            {
                "day": None,
                "days": 12,
                "hour": None,
                "hours": 0,
                "leapdays": 0,
                "microsecond": None,
                "microseconds": 0,
                "minute": None,
                "minutes": 0,
                "month": None,
                "months": 0,
                "second": None,
                "seconds": 0,
                "year": None,
                "years": 0,
            },
            result,
        )

    def test_should_deserialize(self):
        instance = {"__type": "time_delta", "days": 12, "seconds": 0}
        schema_instance = RelativeDeltaSchema()
        result = schema_instance.load(instance)
        expected_instance = relativedelta.relativedelta(days=+12)
        self.assertEqual(expected_instance, result)


class TestCronExpressionSchema(unittest.TestCase):
    def test_should_deserialize(self):
        instance = {"__type": "cron_expression", "value": "5 4 * * *"}
        schema_instance = CronExpressionSchema()
        result = schema_instance.load(instance)
        expected_instance = CronExpression("5 4 * * *")
        self.assertEqual(expected_instance, result)


class TestScheduleIntervalSchema(unittest.TestCase):
    def test_should_serialize_timedelta(self):
        instance = datetime.timedelta(days=12)
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        self.assertEqual({"__type": "time_delta", "days": 12, "seconds": 0}, result)

    def test_should_deserialize_timedelta(self):
        instance = {"__type": "time_delta", "days": 12, "seconds": 0}
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.load(instance)
        expected_instance = datetime.timedelta(days=12)
        self.assertEqual(expected_instance, result)

    def test_should_serialize_relative_delta(self):
        instance = relativedelta.relativedelta(days=+12)
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        self.assertEqual(
            {
                "__type": "relative_delta",
                "day": None,
                "days": 12,
                "hour": None,
                "hours": 0,
                "leapdays": 0,
                "microsecond": None,
                "microseconds": 0,
                "minute": None,
                "minutes": 0,
                "month": None,
                "months": 0,
                "second": None,
                "seconds": 0,
                "year": None,
                "years": 0,
            },
            result,
        )

    def test_should_deserialize_relative_delta(self):
        instance = {"__type": "time_delta", "days": 12, "seconds": 0}
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.load(instance)
        expected_instance = datetime.timedelta(days=12)
        self.assertEqual(expected_instance, result)

    def test_should_serialize_cron_expresssion(self):
        instance = "5 4 * * *"
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        expected_instance = {"__type": "cron_expression", "value": "5 4 * * *"}
        self.assertEqual(expected_instance, result)
