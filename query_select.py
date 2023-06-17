from dataclasses import dataclass
from typing import List, Optional

from db import Database, Direction, Operator, ResultSet


@dataclass
class Where:
    left_hand: str
    right_hand: str
    operator: Operator


@dataclass
class OrderBy:
    field: str
    direction: Direction


@dataclass
class Select:
    fields: List[str]
    table: str
    where: Optional[Where]
    order_by: Optional[OrderBy]
    # limit: Optional[int] = 1000

    def execute(self, db: Database) -> ResultSet:
        table = db.get_table(self.table)

        if self.where:
            rs = table.get_rows_where(
                left_hand=self.where.left_hand,
                right_hand=self.where.right_hand,
                operator=self.where.operator,
            )
        else:
            rs = table.get_rows()

        if self.order_by:
            col_index = rs.headers.index(self.order_by.field)
            rows = sorted(
                rs.rows,
                key=lambda row: row[col_index],
                reverse=self.order_by.direction == "desc",
            )
            rs.rows = rows

        return rs


def parse_select(query: str) -> Select:
    """
    SELECT * FROM users WHERE id = 1 AND age > 18 ORDER BY id DESC
    """
    query = query.lower().replace(";", "").replace("\n", " ").replace("\t", " ")
    parts = query.split(" ")

    if parts[0] != "select":
        raise ValueError("Invalid query")

    fields = []
    for part in parts[1:]:
        if part == "from":
            break

        subparts = part.split(",")

        for subpart in subparts:
            sanitized = subpart.strip()

            if sanitized:
                fields.append(sanitized)

    table = parts[parts.index("from") + 1]

    where = None
    if "where" in parts:
        operator = parts[parts.index("where") + 2]
        if operator not in ["=", ">", "<", ">=", "<="]:
            raise ValueError(f"Invalid operator in WHERE clause ({operator})")

        where = Where(
            left_hand=parts[parts.index("where") + 1],
            operator=parts[parts.index("where") + 2],
            right_hand=parts[parts.index("where") + 3],
        )

    order_by = None
    if "order" in parts:
        direction = parts[parts.index("order") + 3]

        if direction not in ["asc", "desc"]:
            raise ValueError(f"Invalid direction in ORDER BY clause ({direction})")

        order_by = OrderBy(
            field=parts[parts.index("order") + 2],
            direction=parts[parts.index("order") + 3],
        )

    return Select(fields=fields, table=table, where=where, order_by=order_by)
