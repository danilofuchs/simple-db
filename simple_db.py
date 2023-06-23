import argparse
import csv
import dataclasses
import json
import os
import glob
from pathlib import Path
import shutil
from typing import cast
from db import Column, ColumnType, Database, Metadata, Table
from query import QueryType, determine_query_type
from query_insert import parse_insert

from query_select import parse_select
from query_update import parse_update

DATA_DIR = Path(os.path.dirname(__file__)) / "db_data"
META_FILE = DATA_DIR / "meta.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", type=str, help="Execute query")
    parser.add_argument(
        "--import-csv",
        type=str,
        help="Import CSV files from directory",
    )
    args = parser.parse_args()

    if args.import_csv:
        csv_dir = Path(args.import_csv)
        import_csv(csv_dir)
    elif args.execute:
        query = args.execute
        db = restore_db()

        type = determine_query_type(query)

        if type == QueryType.SELECT:
            select = parse_select(query)
            select.set_default_limit(100)
            select.validate(db)
            rs = select.execute(db)
            print(rs)
        if type == QueryType.INSERT:
            insert = parse_insert(query)
            insert.validate(db)
            insert.execute(db)
            print("Inserted row")
        if type == QueryType.UPDATE:
            update = parse_update(query)
            update.validate(db)
            update.execute(db)
            print("Updated row")
    else:
        parser.print_help()


def import_csv(csv_dir: Path):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    meta = Metadata(
        database=Database(
            name=csv_dir.parts[-1],
            tables=[],
        ),
    )

    for file in glob.glob(str(csv_dir) + "/*.csv"):
        file_name = os.path.basename(file)
        table_name = file_name.split(".")[0]

        new_file = DATA_DIR / file_name
        if new_file.exists():
            print(f"File {file_name} already exists, will not overwrite")
            continue

        shutil.copy(file, new_file)
        table = Table(name=table_name, columns=[], file=new_file)

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
                col_type = cast(ColumnType, col_type)

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


def restore_db() -> Database:
    if not os.path.exists(META_FILE):
        raise ValueError("Database not initialized. Please import first")

    with open(META_FILE, "r") as f:
        meta = json.loads(f.read())

    return Database(
        name=meta["database"]["name"],
        tables=[
            Table(
                name=table["name"],
                columns=[
                    Column(name=column["name"], type=column["type"])
                    for column in table["columns"]
                ],
                file=DATA_DIR / table["file"],
            )
            for table in meta["database"]["tables"]
        ],
    )


if __name__ == "__main__":
    main()
