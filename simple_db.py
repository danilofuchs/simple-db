import argparse
import csv
from dataclasses import dataclass
import dataclasses
import json
import os
import glob
from pathlib import Path
import shutil
from typing import Literal

DATA_DIR = Path(os.path.dirname(__file__)) / "db_data"
META_FILE = DATA_DIR / "meta.json"

ColumnType = Literal["int", "str", "date"]


@dataclass
class Column:
    name: str
    type: ColumnType


@dataclass
class Table:
    name: str
    columns: list[Column]


@dataclass
class Database:
    name: str
    tables: list[Table]


@dataclass
class Metadata:
    database: Database


def main():
    query = "SELECT * FROM users WHERE id = 1"
    csv_dir = "/mnt/c/Users/danil/Desktop/employees"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--import-csv", type=str, help="Import CSV files from directory"
    )
    args = parser.parse_args()

    if args.import_csv:
        import_csv(csv_dir)


def import_csv(csv_dir: str):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    meta = Metadata(
        database=Database(
            name=DATA_DIR.parts[-1],
            tables=[],
        ),
    )

    for file in glob.glob(csv_dir + "/*.csv"):
        file_name = os.path.basename(file)
        table_name = file_name.split(".")[0]

        shutil.copy(file, DATA_DIR / file_name)
        table = Table(name=table_name, columns=[])

        with open(file, "r") as f:
            print(f"Importing file {file_name}")
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                print(f"File {file_name} is empty")
                continue

            for column in reader.fieldnames:
                col_type = input(
                    f"Enter type for column {table_name}.{column} (default str): "
                )
                col_type = col_type or "str"

                if col_type not in ["int", "str", "date"]:
                    raise ValueError("Invalid column type")

                table.columns.append(
                    Column(
                        name=column,
                        type=col_type,
                    )
                )

        meta.database.tables.append(table)

    with open(META_FILE, "w") as f:
        f.write(json.dumps(dataclasses.asdict(meta), indent=2))
        print(f"Metadata file saved to {META_FILE}")


if __name__ == "__main__":
    main()
