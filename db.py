import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal
from tabulate import tabulate


ColumnType = Literal["int", "str", "date"]
Operator = Literal["=", ">", "<", ">=", "<="]
Direction = Literal["asc", "desc"]


@dataclass
class Column:
    name: str
    type: ColumnType


@dataclass
class ResultSet:
    headers: List[str]
    rows: List[List[str]]

    def __str__(self) -> str:
        return tabulate(self.rows, self.headers)


@dataclass
class Table:
    name: str
    columns: List[Column]
    file: Path

    def get_headers(self) -> List[str]:
        return [column.name for column in self.columns]

    def get_rows(self) -> ResultSet:
        with open(self.file, "r") as f:
            csv_reader = csv.DictReader(f)
            return ResultSet(
                headers=csv_reader.fieldnames,
                rows=[list(row.values()) for row in csv_reader],
            )

    def get_rows_where(
        self, left_hand: str, right_hand: str, operator: Operator
    ) -> ResultSet:
        return ResultSet(headers=self.get_headers(), rows=[])


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
