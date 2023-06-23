import argparse
import json
import os
from pathlib import Path
from config import META_FILE
from csv_importer import import_csv
from db import Column, Database, Table
from query import QueryType, determine_query_type
from query_insert import parse_insert
from query_select import parse_select
from query_update import parse_update


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

        try:
            if type == QueryType.SELECT:
                select = parse_select(query)
                select.set_default_limit(100)
                select.validate(db)
                rs = select.execute(db)
                print(rs)
            elif type == QueryType.INSERT:
                insert = parse_insert(query)
                insert.validate(db)
                insert.execute(db)
                print("Inserted row")
            elif type == QueryType.UPDATE:
                update = parse_update(query)
                update.validate(db)
                affected = update.execute(db)
                if affected == 1:
                    print(f"Updated 1 row")
                else:
                    print(f"Updated {affected} rows")
        except ValueError as e:
            print(f"[ERROR] {e}")
    else:
        parser.print_help()


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
                file=table["file"],
                next_id=table["next_id"],
            )
            for table in meta["database"]["tables"]
        ],
    )


if __name__ == "__main__":
    main()
