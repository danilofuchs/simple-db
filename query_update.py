import csv
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, List, Optional
from db import Database

from query import Where, parse_where


@dataclass
class Update:
    table: str
    fields: List[str]
    values: List[Any]
    where: Optional[Where]

    def validate(self, db: Database) -> None:
        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        if len(self.fields) != len(self.values):
            raise ValueError(
                f"Number of fields ({len(self.fields)}) and values ({len(self.values)}) don't match"
            )

        table = db.get_table(self.table)
        for index, field in enumerate(self.fields):
            if field not in table.headers:
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

        if self.where:
            if self.where.left_hand not in table.headers:
                raise ValueError(
                    f"Invalid column: {self.where.left_hand} in table {self.table}"
                )

    def execute(self, db: Database) -> int:
        table = db.get_table(self.table)

        rs = table.read()

        affected = 0

        for row in rs.rows:
            for field_index, field in enumerate(self.fields):
                col_index = table.headers.index(field)
                row[col_index] = self.values[field_index]
                affected += 1

            table.save(rs)
        return affected


def parse_update(query: str) -> Update:
    """
    UPDATE users SET name = 'John', age = 18 WHERE id = 1
    """
    query = query.replace(";", "").replace("\n", " ").replace("\t", " ")
    lower = query.lower()

    if not lower.startswith("update"):
        raise ValueError("Invalid query")

    table = re.search(r"update\s+(\w+)\s+set\s+", lower)
    if table is None:
        raise ValueError("Missing table name")
    table = table.group(1)

    has_where = "where" in lower

    set_part_regex = r"set\s+(.*?)\s+where" if has_where else r"set\s+(.*)"
    set_part = re.search(set_part_regex, query, re.IGNORECASE)
    if set_part is None:
        raise ValueError("Missing SET keyword")
    set_part = set_part.group(1)

    fields = []
    values = []
    for field_value in set_part.split(","):
        sanitized = field_value.strip().strip(",")

        if sanitized:
            field, value = sanitized.split("=")
            fields.append(field.strip().lower())

            value = value.strip()
            if value.isnumeric():
                values.append(int(value.strip()))
            elif value.startswith("'") and value.endswith("'"):
                values.append(value.strip().strip("'"))
            elif value.startswith('"') and value.endswith('"'):
                values.append(value.strip().strip('"'))

    where = None
    where_part = re.search(r"where\s+(.*)", query, re.IGNORECASE)
    if where_part is not None:
        where = parse_where(where_part.group(1))

    return Update(table=table, fields=fields, values=values, where=where)
