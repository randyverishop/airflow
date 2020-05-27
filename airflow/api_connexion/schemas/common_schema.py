import datetime

import marshmallow
import typing
from dateutil import relativedelta
from marshmallow import Schema, fields
from marshmallow_oneofschema import OneOfSchema


class CronExpression(typing.NamedTuple):
    value: str


class TimeDeltaSchema(Schema):
    objectType = fields.Constant("time_delta", data_key="__type")
    days = fields.Integer()
    seconds = fields.Integer()
    microsecond = fields.Integer()

    @marshmallow.post_load
    def make_timedelta(self, data, **kwargs):
        if "objectType" in data:
            del data["objectType"]
        return datetime.timedelta(**data)


class RelativeDeltaSchema(marshmallow.Schema):
    objectType = fields.Constant("relative_delta", data_key="__type")
    years = fields.Integer()
    months = fields.Integer()
    days = fields.Integer()
    leapdays = fields.Integer()
    hours = fields.Integer()
    minutes = fields.Integer()
    seconds = fields.Integer()
    microseconds = fields.Integer()
    year = fields.Integer()
    month = fields.Integer()
    day = fields.Integer()
    hour = fields.Integer()
    minute = fields.Integer()
    second = fields.Integer()
    microsecond = fields.Integer()

    @marshmallow.post_load
    def make_timedelta(self, data, **kwargs):
        if "objectType" in data:
            del data["objectType"]

        return relativedelta.relativedelta(**data)


class CronExpressionSchema(Schema):
    objectType = fields.Constant("cron_expression", data_key="__type", required=True)
    value = fields.String(required=True)

    @marshmallow.post_load
    def make_cron_expression(self, data, **kwargs):
        return CronExpression(data["value"])


class ScheduleIntervalSchema(OneOfSchema):
    type_field = "__type"
    type_schemas = {
        "time_delta": TimeDeltaSchema,
        "relative_delta": RelativeDeltaSchema,
        "cron_expression": CronExpressionSchema,
    }

    def _dump(self, obj, *, update_fields=True, **kwargs):
        if isinstance(obj, str):
            obj = CronExpression(obj)

        return super()._dump(obj, update_fields=update_fields, **kwargs)

    def get_obj_type(self, obj):
        if isinstance(obj, datetime.timedelta):
            return "time_delta"
        elif isinstance(obj, relativedelta.relativedelta):
            return "relative_delta"
        elif isinstance(obj, CronExpression):
            return "cron_expression"
        else:
            raise Exception("Unknown object type: {}".format(obj.__class__.__name__))
