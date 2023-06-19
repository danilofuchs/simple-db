import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, List, Literal, Tuple
from tabulate import tabulate


ColumnType = Literal["int", "str", "date"]
Operator = Literal["=", ">", "<", ">=", "<="]
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


@dataclass
class Table:
    name: str
    columns: List[Column]
    file: Path

    def get_headers(self) -> List[str]:
        return [column.name.lower() for column in self.columns]

    def get_rows(self) -> ResultSet:
        with open(self.file, "r") as f:
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

    def __parse_value(self, value: str, column: Column) -> Any:
        if column.type == "int":
            return int(value)
        elif column.type == "date":
            return datetime.strptime(value, "%Y-%m-%d").date()
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
