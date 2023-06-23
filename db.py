import csv
from dataclasses import dataclass
import dataclasses
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, List, Literal, Tuple
from tabulate import tabulate
from config import DATA_DIR, META_FILE

from query import Where


ColumnType = Literal["int", "float", "str", "datetime"]
Direction = Literal["asc", "desc"]


Row = List[Any]


@dataclass
class Column:
    name: str
    type: ColumnType


@dataclass
class ResultSet:
    table_name: str
    columns: List[Column]
    rows: List[Row]

    def __str__(self) -> str:
        return tabulate(self.rows, self.headers)

    @property
    def headers(self) -> List[str]:
        return [column.name.lower() for column in self.columns]

    @staticmethod
    def get_value_of_column(
        row: Row, columns: List[Column], name: str
    ) -> Tuple[Column, Any]:
        for i, column in enumerate(columns):
            if column.name == name:
                return column, row[i]

        raise ValueError(f"Column {name} not found")

    def apply_where(self, where: Where) -> "ResultSet":
        new_rows = []
        for row in self.rows:
            if self.__satisfies_condition(where, row, self.columns):
                new_rows.append(row)

        return ResultSet(self.table_name, self.columns, new_rows)

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

        elif left_hand_col.type == "datetime":
            if not (right_hand.startswith("'") and right_hand.endswith("'")) and not (
                right_hand.startswith('"') and right_hand.endswith('"')
            ):
                raise ValueError(
                    f"Invalid right hand: {right_hand} for datetime comparison"
                )
            right_hand = right_hand.strip("'").strip('"')
            try:
                right_hand = datetime.fromisoformat(right_hand)
            except ValueError:
                raise ValueError(
                    f"Invalid right hand: {right_hand} for datetime comparison"
                )

        elif left_hand_col.type == "int":
            try:
                right_hand = int(right_hand)
            except ValueError:
                raise ValueError(f"Invalid right hand: {right_hand} for int comparison")

        elif left_hand_col.type == "float":
            try:
                right_hand = float(right_hand)
            except ValueError:
                raise ValueError(
                    f"Invalid right hand: {right_hand} for float comparison"
                )

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


@dataclass
class Table:
    name: str
    columns: List[Column]
    file: str
    next_id: int

    @property
    def headers(self) -> List[str]:
        return [column.name.lower() for column in self.columns]

    def read(self) -> ResultSet:
        with open(DATA_DIR / Path(self.file), "r") as f:
            csv_reader = csv.reader(f)
            next(csv_reader, None)  # skip the headers

            rows = []
            for row in csv_reader:
                parsed = []
                for i, col in enumerate(row):
                    parsed.append(self.__parse_value(col, self.columns[i]))
                rows.append(parsed)

            return ResultSet(
                table_name=self.name,
                columns=self.columns,
                rows=rows,
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
            csv_writer.writerows(rs.rows)

    def get_column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column

        raise ValueError(f"Column {name} not found")

    def create_row(self, values: List[str]) -> Row:
        if len(values) != len(self.columns) - 1:
            raise ValueError("Invalid number of values")

        row = [self.next_id]
        for i, value in enumerate(values):
            row.append(self.__parse_value(value, self.columns[i + 1]))
        self.next_id += 1

        return row

    def __parse_value(self, value: str, column: Column) -> Any:
        if column.type == "int":
            return int(value)
        elif column.type == "float":
            return float(value)
        elif column.type == "datetime":
            return datetime.fromisoformat(value)
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
