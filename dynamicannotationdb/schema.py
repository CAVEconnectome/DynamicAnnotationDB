from marshmallow import fields, Schema

class AnnotationSchema(Schema):
    """Annotation schema"""

    id = fields.Int(attribute="id")
    schema = fields.String(attribute="schema")
    user_id = fields.String(attribute="user_id")
    description = fields.String(attribute="description")
    created_on = fields.DateTime(attribute="created_on")
    deleted_on = fields.DateTime(attribute="deleted_on")

