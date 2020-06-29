class TableNameNotFound(KeyError):
    """ Table name is not found in Metadata table """

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
