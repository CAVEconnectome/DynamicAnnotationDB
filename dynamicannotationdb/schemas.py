from flask_marshmallow import Marshmallow
from marshmallow import Schema, ValidationError, fields, validate
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from .models import (
    AnalysisTable,
    AnalysisVersion,
    AnalysisView,
    VersionErrorTable,
)

ma = Marshmallow()

class AnalysisVersionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisVersion
        load_instance = True


class AnalysisTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisTable
        load_instance = True


class AnalysisViewSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisView
        load_instance = True
        fields = ("id", "table_name", "description")
        ordered = True


class VersionErrorTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = VersionErrorTable
        load_instance = True


class CronField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, (str, int, list)):
            return value
        else:
            raise ValidationError("Field should be str, int or list")


class TaskParamsSchema(Schema):
    days_to_expire = fields.Int(
        required=False,
        validate=validate.Range(min=1),
        metadata={
            "description": "Number of days until the materialized database expires"
        },
    )
    merge_tables = fields.Boolean(
        required=False,
        default=False,
        metadata={"description": "Whether to merge tables during materialization"},
    )
    datastack = fields.Str(
        required=False, metadata={"description": "The datastack to use for this task"}
    )
    delete_threshold = fields.Int(
        required=False,
        validate=validate.Range(min=1),
        metadata={"description": "Threshold for deleting expired databases"},
    )


class CeleryBeatSchema(Schema):
    name = fields.Str(required=True, metadata={"description": "Name of the task"})
    minute = CronField(
        default="*", metadata={"description": "Minute field for cron schedule"}
    )
    hour = CronField(
        default="*", metadata={"description": "Hour field for cron schedule"}
    )
    day_of_week = CronField(
        default="*",
        metadata={"description": "Day of week for cron schedule"},
    )
    day_of_month = CronField(
        default="*",
        metadata={"description": "Day of month for cron schedule"},
    )
    month_of_year = CronField(
        default="*",
        metadata={"description": "Month of year for cron schedule"},
    )
    task = fields.Str(required=True, metadata={"description": "Type of task to run"})
    datastack_params = fields.Nested(
        TaskParamsSchema,
        required=False,
        metadata={"description": "Parameters specific to the task"},
    )
