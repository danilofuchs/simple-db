import argparse
import json
import os
from pathlib import Path
import traceback
from config import META_FILE
from csv_importer import import_csv
from db import Column, Database, Metadata, Table
from mysql_importer import import_mysql
from postgres_importer import import_postgres
from query import QueryType, determine_query_type
from query_delete import parse_delete
from query_insert import parse_insert
from query_select import parse_select
from query_update import parse_update


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", type=str, help="Execute query")
    parser.add_argument(
        "--import-csv",
        type=str,
        help="Import CSV files from directory. Input directory name",
    )
    parser.add_argument(
        "--import-pg",
        type=str,
        help="Import from Postgres database. Input connection string (e.g. postgresql://user:password@localhost:5432/database)",
    )
    parser.add_argument(
        "--import-mysql",
        action="store_true",
        help="Import from MySQL database. Use --user, --password, --host, --port, --database args to specify connection details",
    )
    parser.add_argument(
        "--user", type=str, help="User name for MySQL database", default="root"
    )
    parser.add_argument(
        "--password", type=str, help="Password for MySQL database", default="root"
    )
    parser.add_argument(
        "--host", type=str, help="Host for MySQL database", default="localhost"
    )
    parser.add_argument(
        "--port", type=str, help="Port for MySQL database", default="3306"
    )
    parser.add_argument(
        "--database",
        type=str,
        help="Database name for MySQL database",
    )
    args = parser.parse_args()

    if args.import_csv:
        csv_dir = Path(args.import_csv)
        import_csv(csv_dir)
    elif args.import_pg:
        connection_string = args.import_pg
        import_postgres(connection_string)
    elif args.import_mysql:
        user = args.user
        password = args.password
        host = args.host
        port = args.port
        database = args.database
        import_mysql(user, password, host, port, database)
    elif args.execute:
        query = args.execute
        meta = Metadata.load()
        db = meta.database

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
                meta.save()
                print("Inserted row")
            elif type == QueryType.UPDATE:
                update = parse_update(query)
                update.validate(db)
                affected = update.execute(db)
                meta.save()
                if len(affected) == 0:
                    print("No rows updated")
                elif len(affected) == 1:
                    print(f"Updated 1 row: __id={affected[0]}")
                else:
                    print(f"Updated {len(affected)} rows: __id={affected}")
            elif type == QueryType.DELETE:
                delete = parse_delete(query)
                delete.validate(db)
                affected = delete.execute(db)
                meta.save()
                if len(affected) == 0:
                    print("No rows deleted")
                elif len(affected) == 1:
                    print(f"Deleted 1 row: __id={affected[0]}")
                else:
                    print(f"Deleted {len(affected)} rows: __id={affected}")
        except ValueError as e:
            print(f"[ERROR] {e} \n\n {traceback.format_exc()}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
