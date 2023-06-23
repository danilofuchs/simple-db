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
    """
    id = 1
    name = 'Fuchs'
    id=1
    age > 18
    salary <= 1000.00
    """
    query_part = query_part.strip()

    if ">=" in query_part:
        left_hand, right_hand = query_part.split(">=")
        operator = ">="
    elif "<=" in query_part:
        left_hand, right_hand = query_part.split("<=")
        operator = "<="
    elif "=" in query_part:
        left_hand, right_hand = query_part.split("=")
        operator = "="
    elif ">" in query_part:
        left_hand, right_hand = query_part.split(">")
        operator = ">"
    elif "<" in query_part:
        left_hand, right_hand = query_part.split("<")
        operator = "<"
    else:
        raise ValueError(f"Invalid WHERE clause ({query_part})")

    return Where(
        left_hand=left_hand.strip(),
        right_hand=right_hand.strip(),
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
