from dynamicannotationdb import db
from typing import List
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.interface import AnnotationInterface


class AnnotationService:
    @staticmethod
    def get_all() -> List[Annotation]:
        return Annotation.query.all()

    @staticmethod
    def get_by_id(id: int) -> Annotation:
        return Annotation.query.get(id)

    @staticmethod
    def update(annotation: Annotation, annotation_change_updates: AnnotationInterface) -> Annotation:
        annotation.update(annotation_change_updates)
        db.session.commit()
        return annotation

    @staticmethod
    def delete_by_id(id: int) -> List[int]:
        annotation = Annotation.query.filter(Annotation.id == id).first()
        if not annotation:
            return []
        db.session.delete(annotation)
        db.session.commit()
        return [id]

    @staticmethod
    def create(new_attrs: AnnotationInterface) -> Annotation:
        new_annotiation = Annotation(schema=new_attrs["schema"],
                                     user_id=new_attrs["user_id"],
                                     description=new_attrs["description"],
                                     created_on=new_attrs["created_on"],
                                     )
        db.session.add(new_annotiation)
        db.session.commit()

        return new_annotiation
