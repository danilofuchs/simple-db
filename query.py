from dataclasses import dataclass
from enum import Enum
import re
from typing import Literal, Optional


Operator = Literal["=", ">", "<", ">=", "<="]


@dataclass
class Where:
    left_hand: str
    right_hand: str
    operator: Operator
    or_where: Optional["Where"]
    and_where: Optional["Where"]


def parse_where(query_part: str):
    """
    id = 1
    name = 'Fuchs'
    id=1
    age > 18
    salary <= 1000.00
    id = 1 OR name = 'Fuchs'
    """
    query_part = query_part.strip()
    lower = query_part.lower()

    if lower.count(" or ") + lower.count(" and ") > 1:
        raise ValueError(
            f"Invalid WHERE clause. Only two conditions are supported. ({query_part})"
        )

    if " or " in query_part.lower():
        conditions = re.search(r"(.*) or (.*)", query_part, re.IGNORECASE)

        if conditions is None:
            raise ValueError(f"Invalid WHERE clause ({query_part})")

        first_condition = conditions.group(1)
        second_condition = conditions.group(2)

        first_where = __parse_condition(first_condition)
        first_where.or_where = __parse_condition(second_condition)

        return first_where

    elif " and " in query_part.lower():
        conditions = re.search(r"(.*) and (.*)", query_part, re.IGNORECASE)

        if conditions is None:
            raise ValueError(f"Invalid WHERE clause ({query_part})")

        first_condition = conditions.group(1)
        second_condition = conditions.group(2)

        first_where = __parse_condition(first_condition)
        first_where.and_where = __parse_condition(second_condition)

        return first_where

    return __parse_condition(query_part)


def __parse_condition(query_part: str):
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
        or_where=None,
        and_where=None,
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


def is_quoted_string(string: str) -> bool:
    return (string[0] == '"' and string[-1] == '"') or (
        string[0] == "'" and string[-1] == "'"
    )
