import pytest
from dynamicannotationdb.errors import (
    TableNameNotFound,
    UpdateAnnotationError,
    AnnotationInsertLimitExceeded,
    NoAnnotationsFoundWithID,
)


def table_not_found():
    raise TableNameNotFound("test_table")


def update_annotation_error():
    raise UpdateAnnotationError(1, 3)


def annotation_insert_limit():
    raise AnnotationInsertLimitExceeded(100, 1000)


def no_annotation_found_with_id():
    raise NoAnnotationsFoundWithID(1)


def test_table_name_not_found():
    with pytest.raises(TableNameNotFound) as excinfo:
        table_not_found()
    assert excinfo.value.message == "No table named 'test_table' exists."


def test_update_annotation_error():
    with pytest.raises(UpdateAnnotationError) as excinfo:
        update_annotation_error()

    assert (
        excinfo.value.message
        == "Annotation with ID 1 has already been superseded by annotation ID 3, update annotation ID 3 instead"
    )


def test_annotation_insert_limit_exceeded():
    with pytest.raises(AnnotationInsertLimitExceeded) as excinfo:
        annotation_insert_limit()

    assert (
        excinfo.value.message
        == "The insertion limit is 100, 1000 were attempted to be inserted"
    )


def test_no_annotations_found_with_id():
    with pytest.raises(NoAnnotationsFoundWithID) as excinfo:
        no_annotation_found_with_id()

    assert excinfo.value.message == "No annotation with 1 exists"
