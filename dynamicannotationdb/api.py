from flask import request, jsonify
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from flask.wrappers import Response
from typing import List


from dynamicannotationdb.schema import AnnotationSchema
from dynamicannotationdb.service import AnnotationService
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.interface import AnnotationInterface

api = Namespace("Annotation", description="Dynamic Annotation DB")

@api.route("/")
class AnnotationResource(Resource):
    """Annotations"""

    @responds(schema=AnnotationSchema, many=True)
    def get(self) -> List[Annotation]:
        """Get all Annotation Tables"""

        return AnnotationService.get_all()

    @accepts(schema=AnnotationSchema, api=api)
    @responds(schema=AnnotationSchema)
    def post(self) -> Annotation:
        """Create a Single Annotation"""

        return AnnotationService.create(request.parsed_obj)


@api.route("/<int:id>")
@api.param("id", "Annotation database ID")
class AnnotationIdResource(Resource):
    @responds(schema=AnnotationSchema)
    def get(self, id: int) -> Annotation:
        """Get Single Annotation"""

        return AnnotationService.get_by_id(id)

    def delete(self, id: int) -> Response:
        """Delete Single Annotation"""

        id = AnnotationService.delete_by_id(id)
        return jsonify(dict(status="Success", id=id))

    @accepts(schema=AnnotationSchema, api=api)
    @responds(schema=AnnotationSchema)
    def put(self, id: int) -> Annotation:
        """Update Single Annotation"""

        changes: AnnotationInterface = request.parsed_obj
        Annotation = AnnotationService.get_by_id(id)
        return AnnotationService.update(Annotation, changes)
