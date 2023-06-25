import csv
from dataclasses import dataclass
import dataclasses
from datetime import datetime
import functools
import json
import os
from pathlib import Path
from typing import Any, List, Literal, Tuple, Union
from tabulate import tabulate
from config import DATA_DIR, META_FILE

from query import Where, is_quoted_string, unquote_string


ColumnTypeName = Literal["int", "float", "str", "datetime"]
ColumnType = Union[int, float, str, datetime]
Direction = Literal["asc", "desc"]


Row = Tuple[ColumnType]


@dataclass
class Column:
    name: str
    type: ColumnTypeName


@dataclass
class ResultSet:
    table_name: str
    columns: Tuple[Column]
    rows: Tuple[Row]

    def __str__(self) -> str:
        return tabulate(self.rows, self.headers)

    @property
    def headers(self) -> List[str]:
        return [column.name.lower() for column in self.columns]

    def inner_join(self, other: "ResultSet", on: Where) -> "ResultSet":
        new_rows = []
        joined_columns = tuple(self.columns + other.columns)
        joined_column_names = tuple(column.name for column in joined_columns)
        for row in self.rows:
            for other_row in other.rows:
                joined = tuple(row + other_row)
                if self.__satisfies_condition(
                    on,
                    joined,
                    joined_columns,
                    joined_column_names,
                ):
                    new_rows.append(joined)

        return ResultSet(
            f"{self.table_name} INNER JOIN {other.table_name}",
            joined_columns,
            tuple(new_rows),
        )

    def where(self, where: Where) -> "ResultSet":
        new_rows = []
        columns = tuple(self.columns)
        column_names = tuple(column.name for column in columns)
        for row in self.rows:
            if where.and_where:
                if self.__satisfies_condition(
                    where,
                    row,
                    columns,
                    column_names,
                ) and self.__satisfies_condition(
                    where.and_where,
                    row,
                    columns,
                    column_names,
                ):
                    new_rows.append(row)
            elif where.or_where:
                if self.__satisfies_condition(
                    where,
                    row,
                    columns,
                    column_names,
                ) or self.__satisfies_condition(
                    where.or_where,
                    row,
                    columns,
                    column_names,
                ):
                    new_rows.append(row)
            elif self.__satisfies_condition(
                where,
                row,
                columns,
                column_names,
            ):
                new_rows.append(row)

        return ResultSet(self.table_name, self.columns, tuple(new_rows))

    def __satisfies_condition(
        self, where: Where, row: Row, columns: Tuple[Column], column_names: Tuple[str]
    ) -> bool:
        i = get_column_index(column_names, where.left_hand)
        left_hand_val = row[i]
        left_hand_col = columns[i]

        try:
            i = get_column_index(column_names, where.right_hand)
            right_hand_val = row[i]
        except ValueError:
            # If the right hand is not a column, it must be a value
            right_hand_val = where.right_hand
            if left_hand_col.type == "str":
                right_hand_val = unquote_string(right_hand_val)
            elif left_hand_col.type == "int":
                right_hand_val = int(right_hand_val)
            elif left_hand_col.type == "float":
                right_hand_val = float(right_hand_val)
            elif left_hand_col.type == "datetime":
                right_hand_val = to_datetime(unquote_string(right_hand_val))

        if where.operator == "=":
            return left_hand_val == right_hand_val
        elif where.operator == ">":
            return left_hand_val > right_hand_val
        elif where.operator == "<":
            return left_hand_val < right_hand_val
        elif where.operator == ">=":
            return left_hand_val >= right_hand_val
        elif where.operator == "<=":
            return left_hand_val <= right_hand_val
        elif where.operator == "!=":
            return left_hand_val != right_hand_val
        else:
            raise ValueError(f"Invalid operator: {where.operator} for comparison")


@dataclass
class Table:
    name: str
    columns: List[Column]
    file: str
    next_id: int

    @property
    def headers(self) -> List[str]:
        return [column.name.lower() for column in self.columns]

    @property
    def prefixed_headers(self) -> List[str]:
        return [f"{self.name}.{column.name.lower()}" for column in self.columns]

    def read(self, prefixed=False) -> ResultSet:
        with open(DATA_DIR / Path(self.file), "r") as f:
            csv_reader = csv.reader(f)
            next(csv_reader, None)  # skip the headers

            rows = []
            for row in csv_reader:
                parsed = []
                for i, col in enumerate(row):
                    parsed.append(parse_value(col, self.columns[i]))
                rows.append(tuple(parsed))

            if prefixed:
                columns = [
                    Column(f"{self.name}.{column.name}", column.type)
                    for column in self.columns
                ]
            else:
                columns = self.columns

            return ResultSet(
                table_name=self.name,
                columns=tuple(columns),
                rows=tuple(rows),
            )

    def write(self, rs: ResultSet) -> None:
        if rs.table_name != self.name:
            raise ValueError(f"Cannot save ResultSet from table {rs.table_name}")

        if rs.headers != self.headers:
            raise ValueError("Columns do not match")

        with open(DATA_DIR / self.file, "w") as f:
            csv_writer = csv.writer(
                f,
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                delimiter=",",
                lineterminator="\n",
            )
            csv_writer.writerow(self.headers)
            for row in rs.rows:
                str_row = [
                    value.isoformat()
                    if isinstance(value, datetime)
                    else f"{value:.4f}"
                    if isinstance(value, float)
                    else str(value)
                    if value is not None
                    else ""
                    for value in row
                ]
                csv_writer.writerow(str_row)

    def get_column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column

        raise ValueError(f"Column {name} not found in table {self.name}")

    def create_row(self, values: Tuple[str], fields: Tuple[str]) -> Row:
        row = []
        for col in self.columns:
            if col.name == "__id":
                row.append(self.next_id)
                continue

            try:
                values_index = get_column_index(fields, col.name)
            except ValueError:
                # If the column is not in the fields, it must be a default value
                row.append("")
                continue
            parsed = parse_value(values[values_index], col)
            row.append(parsed)

        self.next_id += 1

        return tuple(row)


def parse_value(value: str, column: Column) -> ColumnType:
    if value == "":
        if column.type == "datetime":
            return datetime(1970, 1, 1, 0, 0, 0, 0)
        elif column.type == "int":
            return 0
        elif column.type == "float":
            return 0.0
        else:
            return ""
    if column.type == "int":
        return int(value)
    elif column.type == "float":
        return float(value)
    elif column.type == "str":
        return unquote_string(value)
    elif column.type == "datetime":
        return datetime.fromisoformat(unquote_string(value))
    else:
        return value


@dataclass
class Database:
    name: str
    tables: List[Table]

    def get_table(self, name: str) -> Table:
        for table in self.tables:
            if table.name == name:
                return table

        raise ValueError(f"Table {name} not found")


@dataclass
class Metadata:
    database: Database

    def save(self):
        with open(META_FILE, "w") as f:
            f.write(json.dumps(dataclasses.asdict(self), indent=2))

    @staticmethod
    def load() -> "Metadata":
        if not os.path.exists(META_FILE):
            raise ValueError("Database not initialized. Please import first")

        with open(META_FILE, "r") as f:
            meta = json.loads(f.read())

        return Metadata(
            database=Database(
                name=meta["database"]["name"],
                tables=[
                    Table(
                        name=table["name"],
                        columns=[
                            Column(name=column["name"], type=column["type"])
                            for column in table["columns"]
                        ],
                        file=table["file"],
                        next_id=table["next_id"],
                    )
                    for table in meta["database"]["tables"]
                ],
            ),
        )


@functools.lru_cache(maxsize=None)
def get_column_index(column_names: Tuple[str], name: str) -> int:
    try:
        return column_names.index(name)
    except ValueError:
        raise ValueError(f"Column {name} not found")


@functools.lru_cache(maxsize=None)
def to_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def validate_where(
    where: Where, db: Database, headers: List[str], table_name: str
) -> None:
    left_hand = where.left_hand
    right_hand = where.right_hand

    if left_hand not in headers:
        raise ValueError(f"Invalid column for where: {left_hand}")

    if "." in left_hand:
        left_table_name, left_hand_col_name = left_hand.split(".")
    else:
        left_table_name = table_name
        left_hand_col_name = left_hand

    left_table = db.get_table(left_table_name)
    left_col = left_table.get_column(left_hand_col_name)

    if right_hand in headers:
        if "." in right_hand:
            right_table_name, right_hand_col_name = right_hand.split(".")
        else:
            right_table_name = left_table_name
            right_hand_col_name = right_hand

        if right_table_name not in [table.name for table in db.tables]:
            raise ValueError(f"Invalid table in where: {right_table_name}")

        right_table = db.get_table(right_table_name)
        right_col = right_table.get_column(right_hand_col_name)

        if left_col.type != right_col.type:
            raise ValueError(
                f"Invalid type for where: {left_col.type} != {right_col.type}"
            )
    else:
        if left_col.type == "str":
            if not is_quoted_string(right_hand):
                raise ValueError(f"Invalid string for where: {right_hand}")
        elif left_col.type == "int":
            try:
                int(right_hand)
            except ValueError:
                raise ValueError(f"Invalid int for where: {right_hand}")
        elif left_col.type == "float":
            try:
                float(right_hand)
            except ValueError:
                raise ValueError(f"Invalid float for where: {right_hand}")
        elif left_col.type == "datetime":
            if not is_quoted_string(right_hand):
                raise ValueError(f"Invalid string for where: {right_hand}")
            try:
                datetime.fromisoformat(right_hand.strip('"').strip("'"))
            except ValueError:
                raise ValueError(f"Invalid datetime for where: {right_hand}")

    if where.and_where:
        validate_where(where.and_where, db, headers, table_name)

    if where.or_where:
        validate_where(where.or_where, db, headers, table_name)
