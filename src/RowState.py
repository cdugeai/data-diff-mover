from enum import Enum


class RowState(Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    UNCHANGED = "unchanged"
    UNDEFINED = "undefined"
