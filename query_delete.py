from dataclasses import dataclass
from datetime import datetime
import re
from typing import List, Optional
from db import Database

from query import Where, parse_where


@dataclass
class Delete:
    table: str
    where: Optional[Where]

    def validate(self, db: Database) -> None:
        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        table = db.get_table(self.table)

        if self.where:
            if self.where.left_hand not in table.headers:
                raise ValueError(
                    f"Invalid column: {self.where.left_hand} in table {self.table}"
                )

    def execute(self, db: Database) -> List[int]:
        table = db.get_table(self.table)

        rs = table.read()

        if self.where:
            filtered = rs.where(self.where)
        else:
            filtered = rs

        affected_ids = [row[0] for row in filtered.rows]

        rs.rows = [row for row in rs.rows if row[0] not in affected_ids]

        table.write(rs)

        return affected_ids


def parse_delete(query: str) -> Delete:
    """
    DELETE FROM users WHERE id = 1
    """
    query = query.replace(";", "").replace("\n", " ").replace("\t", " ")
    lower = query.lower()

    if not lower.startswith("delete from"):
        raise ValueError("Invalid query")

    has_where = "where" in lower
    table_regex = r"from\s+(\w+)\s+where\s+" if has_where else r"from\s+(\w+)"
    table = re.search(table_regex, lower)
    if table is None:
        raise ValueError("Missing table name")
    table = table.group(1)

    where = None
    where_part = re.search(r"where\s+(.*)", query, re.IGNORECASE)
    if where_part is not None:
        where = parse_where(where_part.group(1))

    return Delete(table=table, where=where)
