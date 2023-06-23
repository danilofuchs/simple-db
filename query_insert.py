from dataclasses import dataclass
import re
from typing import Any, List


@dataclass
class Insert:
    table: str
    fields: List[str]
    values: List[Any]


def parse_insert(query: str) -> Insert:
    """
    INSERT INTO users (id, name) VALUES (1, "John")
    """
    lower = query.lower().replace(";", "").replace("\n", " ").replace("\t", " ")

    if not lower.startswith("insert into"):
        raise ValueError("Invalid query")

    between_parenthesis: List[str] = re.findall(r"\((.*?)\)", query)

    if len(between_parenthesis) != 2:
        raise ValueError("Query must have exactly two sets of parentheses")

    table = re.search(r"into\s+(\w+)\s*\(", lower)
    if table is None:
        raise ValueError("Missing table name")
    table = table.group(1)

    fields = []
    for fields_part in between_parenthesis[0].split(","):
        sanitized = fields_part.strip("(),").strip()

        if sanitized:
            fields.append(sanitized.lower())

    if re.search(r"\)\s+values\s*\(", lower) is None:
        raise ValueError("Missing VALUES keyword")

    values = []
    for values_part in between_parenthesis[1].split(","):
        sanitized = values_part.strip("(),").strip()

        if sanitized:
            if sanitized.isnumeric():
                sanitized = int(sanitized)
            elif sanitized.startswith('"') and sanitized.endswith('"'):
                sanitized = sanitized.strip('"')
            elif sanitized.startswith("'") and sanitized.endswith("'"):
                sanitized = sanitized.strip("'")
            else:
                raise ValueError(f"Invalid value: {sanitized}")
            values.append(sanitized)

    return Insert(
        table=table,
        fields=fields,
        values=values,
    )
