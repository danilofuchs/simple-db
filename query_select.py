from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, cast

from db import Column, Database, Direction, Operator, ResultSet
from query import Where


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
    limit: Optional[int]

    def validate(self, db: Database) -> None:
        if self.table not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table: {self.table}")

        if self.where:
            if self.where.left_hand not in db.get_table(self.table).get_headers():
                raise ValueError(
                    f"Invalid column: {self.where.left_hand} in table {self.table}"
                )

        if self.order_by:
            if self.order_by.field not in db.get_table(self.table).get_headers():
                raise ValueError(
                    f"Invalid column: {self.order_by.field} in table {self.table}"
                )

        if self.limit:
            if self.limit < 0:
                raise ValueError(f"Invalid limit: {self.limit}")

        if self.fields != ["*"]:
            for field in self.fields:
                if field not in db.get_table(self.table).get_headers():
                    raise ValueError(f"Invalid column: {field} in table {self.table}")

    def execute(self, db: Database) -> ResultSet:
        table = db.get_table(self.table)

        rs = table.get_rows()
        if self.where:
            col_index = rs.headers.index(self.where.left_hand)
            rs.rows = [
                row
                for row in rs.rows
                if self.__satisfies_condition(self.where, row, rs.columns)
            ]

        if self.fields == ["*"]:
            self.fields = table.get_headers()
            rs.columns = table.columns
        else:
            col_indexes = [table.get_headers().index(field) for field in self.fields]
            rs.rows = [[row[i] for i in col_indexes] for row in rs.rows]
            rs.columns = [table.columns[i] for i in col_indexes]

        if self.order_by:
            if self.order_by.field not in rs.headers:
                raise ValueError(
                    f"Invalid column: {self.order_by.field} in table {self.table}"
                )
            col_index = rs.headers.index(self.order_by.field)
            rows = sorted(
                rs.rows,
                key=lambda row: row[col_index],
                reverse=self.order_by.direction == "desc",
            )
            rs.rows = rows

        if self.limit:
            rs.rows = rs.rows[: self.limit]

        return rs

    def __satisfies_condition(
        self, where: Where, row: List[str], columns: List[Column]
    ) -> bool:
        left_hand_col, left_hand_val = ResultSet.get_value_of_column(
            row, columns, where.left_hand
        )
        right_hand = where.right_hand

        if left_hand_col.type == "str":
            if not (right_hand.startswith("'") and right_hand.endswith("'")) and not (
                right_hand.startswith('"') and right_hand.endswith('"')
            ):
                raise ValueError(
                    f"Invalid right hand: {right_hand} for string comparison"
                )
            right_hand = right_hand.strip("'").strip('"')

        elif left_hand_col.type == "date":
            if not (right_hand.startswith("'") and right_hand.endswith("'")) and not (
                right_hand.startswith('"') and right_hand.endswith('"')
            ):
                raise ValueError(
                    f"Invalid right hand: {right_hand} for date comparison"
                )
            right_hand = right_hand.strip("'").strip('"')
            right_hand = datetime.strptime(right_hand, "%Y-%m-%d").date()

        elif left_hand_col.type == "int":
            try:
                right_hand = int(right_hand)
            except ValueError:
                raise ValueError(f"Invalid right hand: {right_hand} for int comparison")

        if where.operator == "=":
            return left_hand_val == right_hand
        elif where.operator == ">":
            return left_hand_val > right_hand
        elif where.operator == "<":
            return left_hand_val < right_hand
        elif where.operator == ">=":
            return left_hand_val >= right_hand
        elif where.operator == "<=":
            return left_hand_val <= right_hand
        else:
            raise ValueError(
                f"Invalid operator: {where.operator} for numeric comparison"
            )

    def set_default_limit(self, limit: int) -> None:
        if not self.limit:
            self.limit = limit


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
        operator = cast(Operator, operator)

        where = Where(
            left_hand=parts[parts.index("where") + 1],
            operator=operator,
            right_hand=parts[parts.index("where") + 3],
        )

    order_by = None
    if "order" in parts:
        direction = parts[parts.index("order") + 3]

        if direction not in ["asc", "desc"]:
            raise ValueError(f"Invalid direction in ORDER BY clause ({direction})")

        direction = cast(Direction, direction)

        order_by = OrderBy(
            field=parts[parts.index("order") + 2],
            direction=direction,
        )

    limit = None
    if "limit" in parts:
        limit = int(parts[parts.index("limit") + 1])

    return Select(
        fields=fields, table=table, where=where, order_by=order_by, limit=limit
    )
