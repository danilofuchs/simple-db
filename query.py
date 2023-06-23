from enum import Enum


class QueryType(Enum):
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


def determine_query_type(query: str):
    query = query.lower()

    if query.startswith("select"):
        return QueryType.SELECT
    elif query.startswith("insert"):
        return QueryType.INSERT
    elif query.startswith("update"):
        return QueryType.UPDATE
    elif query.startswith("delete"):
        return QueryType.DELETE
