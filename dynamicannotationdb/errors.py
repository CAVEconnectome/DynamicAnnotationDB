class TableNameNotFound(KeyError):
    """ Table name is not found in the Metadata table """

class TableAlreadyExists(KeyError):
    """ Table name already exists in the Metadata table """

class IdsAlreadyExists(KeyError):
    """ Annotation IDs already exists in the segmentation table """


class BadRequest(Exception):
    pass


class UpdateAnnotationError(ValueError):
    
    def __init__(self, target_id: int, superceded_id: int, message: str ="Annotation limit exceeded"):
        self.target_id = target_id
        self.message = f"""Annotation with ID {target_id} has already been superseded by annotation ID {superceded_id},
                        update annotation ID {superceded_id} instead"""
        super().__init__(self.message)

    def __str__(self):
        return f"Error update ID {self.target_id} -> {self.message}"

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