from dataclasses import dataclass
from datetime import datetime

import re
from typing import Any, List

from db import Database


@dataclass
class Insert:
    table: str
    fields: List[str]
    values: List[Any]

    def validate(self, db: Database) -> None:
        if len(self.fields) != len(self.values):
            raise ValueError("Number of fields and values must match")

        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        table = db.get_table(self.table)
        for index, field in enumerate(self.fields):
            if field not in table.get_headers():
                raise ValueError(f"Invalid column: {field} in table {self.table}")

            value = self.values[index]
            col = table.get_column(field)
            if col.type == "str" and not isinstance(value, str):
                raise ValueError(f"Invalid type for column {field}: {type(value)}")
            elif col.type == "int" and not isinstance(value, int):
                raise ValueError(f"Invalid type for column {field}: {type(value)}")
            elif col.type == "date":
                if not isinstance(value, str):
                    raise ValueError(f"Invalid type for column {field}: {type(value)}")

                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Invalid date format for column {field}: {value}")

    def execute(self, db: Database) -> None:
        table = db.get_table(self.table)

        with open(table.file, "a") as f:
            f.write("\n")
            for i, field in enumerate(table.get_headers()):
                if field in self.fields:
                    f.write(str(self.values[self.fields.index(field)]))
                else:
                    f.write("")

                if i != len(table.get_headers()) - 1:
                    f.write(",")


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
