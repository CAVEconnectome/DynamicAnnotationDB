class TableNameNotFound(KeyError):
    """ Table name is not found in the Metadata table """

class TableAlreadyExists(KeyError):
    """ Table name already exists in the Metadata table """

class BadRequest(Exception):
    pass

class AnnotationInsertLimitExceeded(ValueError):
    """Exception raised when amount of annotations exceeds defined limit.
    """

    def __init__(self, limit: int, length: int, message: str ="Annotation limit exceeded"):
        self.limit = limit
        self.message = f"The insertion limit is {limit}, {length} were attempted to be inserted"
        super().__init__(self.message)

    def __str__(self):
        return f"{self.limit} -> {self.message}"


class NoAnnotationsFoundWithID(Exception):
    """No annotation found with specified ID 
    """

    def __init__(self, anno_id: int):
        self.anno_id = anno_id
        self.message = f"No annotation with {anno_id} exists"
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}"