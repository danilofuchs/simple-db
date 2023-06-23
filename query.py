from dataclasses import dataclass
from enum import Enum
from typing import cast

from db import Operator


@dataclass
class Where:
    left_hand: str
    right_hand: str
    operator: Operator


def parse_where(query_part: str):
    query_part = query_part.strip()
    left_hand, operator, right_hand = query_part.split(" ")

    if operator not in ["=", ">", "<", ">=", "<="]:
        raise ValueError(f"Invalid operator in WHERE clause ({operator})")
    operator = cast(Operator, operator)

    return Where(
        left_hand=left_hand,
        right_hand=right_hand,
        operator=operator,
    )


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
