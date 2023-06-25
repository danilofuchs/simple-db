from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, List, Optional
from db import Database, parse_value, validate_where

from query import Where, is_quoted_string, parse_where, unquote_string


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
            if col.type == "str":
                if not is_quoted_string(value):
                    raise ValueError(f"Invalid string for column {field}: {value}")
            elif col.type == "int":
                try:
                    int(value)
                except ValueError:
                    raise ValueError(f"Invalid int for column {field}: {value}")
            elif col.type == "float":
                try:
                    float(value)
                except ValueError:
                    raise ValueError(f"Invalid float for column {field}: {value}")
            elif col.type == "datetime":
                if not is_quoted_string(value):
                    raise ValueError(f"Invalid string for column {field}: {value}")

                try:
                    datetime.fromisoformat(unquote_string(value))
                except ValueError:
                    raise ValueError(f"Invalid date for column {field}: {value}")

        if self.where:
            validate_where(self.where, db, table.headers, table.name)

    def execute(self, db: Database) -> List[int]:
        table = db.get_table(self.table)

        rs = table.read()

        if self.where:
            filtered = rs.where(self.where)
        else:
            filtered = rs

        affected_ids = [row[0] for row in filtered.rows]

        for id in affected_ids:
            if not isinstance(id, int):
                raise ValueError(f"Invalid id {id} in table {self.table}")

        new_rows = []
        for row in rs.rows:
            if row[0] in affected_ids:
                mutable_row = list(row)
                for field_index, field in enumerate(self.fields):
                    col_index = table.headers.index(field)
                    mutable_row[col_index] = parse_value(
                        self.values[field_index], table.columns[col_index]
                    )
                new_rows.append(mutable_row)
            else:
                new_rows.append(row)

        rs.rows = tuple(new_rows)
        table.write(rs)

        return affected_ids


def parse_update(query: str) -> Update:
    """
    UPDATE users SET name = 'John', age = 18 WHERE id = 1
    """
    query = query.replace(";", "").replace("\n", " ").replace("\t", " ").strip()
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
            values.append(value.strip())

    where = None
    where_part = re.search(r"where\s+(.*)", query, re.IGNORECASE)
    if where_part is not None:
        where = parse_where(where_part.group(1))

    return Update(table=table, fields=fields, values=values, where=where)
