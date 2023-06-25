import os
from typing import Any, List
import psycopg

from config import DATA_DIR
from db import Column, Database, Metadata, ResultSet, Table


def import_postgres(connection_string: str):
    if os.path.exists(DATA_DIR):
        if len(os.listdir(DATA_DIR)) > 0:
            print("Data directory is not empty, will not overwrite")
            return
    else:
        os.makedirs(DATA_DIR)

    with psycopg.connect(connection_string) as conn:
        meta = Metadata(
            database=Database(
                name=psycopg.ConnectionInfo(conn.pgconn).dbname,
                tables=[],
            ),
        )
        meta.save()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """,
            )
            table_names = cursor.fetchall()
            for table_name in table_names:
                table_name = table_name[0].decode("utf-8")

                ask = input(f"Import table {table_name}? (Y/n)")
                if ask.lower() == "n":
                    continue

                file_name = f"{table_name}.csv"
                new_file = DATA_DIR / file_name
                if new_file.exists():
                    print(f"File {file_name} already exists, will not overwrite")
                    continue

                table = Table(
                    name=table_name,
                    columns=[
                        Column("__id", type="int"),
                    ],
                    file=new_file.relative_to(DATA_DIR).as_posix(),
                    next_id=0,
                )

                cursor.execute(
                    """
                    SELECT column_name, udt_name, numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )

                columns = cursor.fetchall()

                for column_infos in columns:
                    column_name: str = column_infos[0].decode("utf-8")
                    column_type: str = column_infos[1].decode("utf-8")
                    column_numeric_scale: int = column_infos[2]

                    parsed_type = None
                    if column_type in ["varchar", "bpchar", "text", "uuid"]:
                        parsed_type = "str"
                    elif column_type.startswith("float"):
                        parsed_type = "float"
                    elif column_type == "numeric":
                        if column_numeric_scale == 0:
                            parsed_type = "int"
                        else:
                            parsed_type = "float"
                    elif column_type == "timestamp":
                        parsed_type = "datetime"
                    else:
                        raise ValueError(f"Incompatible column type {column_type}")

                    table.columns.append(
                        Column(
                            name=column_name,
                            type=parsed_type,
                        )
                    )

                print(f"Importing data from {table_name}")
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                imported_rows = []
                for i, row in enumerate(rows):
                    table.next_id = i
                    imported_row: List[Any] = [table.next_id]

                    for col, value in enumerate(row):
                        col = col + 1  # Skip __id
                        if isinstance(value, bytes):
                            value = value.decode("utf-8")

                        if table.columns[col].type == "datetime":
                            pass  # datetime is already parsed
                        elif table.columns[col].type == "int":
                            value = int(value)
                        elif table.columns[col].type == "float":
                            value = float(value)

                        imported_row.append(value)
                    imported_rows.append(imported_row)

                table.next_id += 1
                table.write(
                    ResultSet(
                        table_name=table_name,
                        columns=tuple(table.columns),
                        rows=tuple(imported_rows),
                    )
                )
                meta.database.tables.append(table)
                meta.save()
