from config import DATA_DIR, META_FILE
from db import Column, ColumnType, Database, Metadata, ResultSet, Table


import csv
import dataclasses
import glob
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, List, cast


def import_csv(csv_dir: Path):
    print("Importing CSV files from directory", csv_dir)
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    meta = Metadata(
        database=Database(
            name=csv_dir.parts[-1],
            tables=[],
        ),
    )
    __save_meta(meta)

    for file in glob.glob(str(csv_dir) + "/*.csv"):
        file_name = os.path.basename(file)
        table_name = file_name.split(".")[0]

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

        with open(file, "r") as f:
            print(f"Importing file {file_name}")
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                print(f"File {file_name} is empty")
                continue

            for column in reader.fieldnames:
                col_type = input(
                    f"Enter type for column {table_name}.{column} (int, float, str (default), datetime): "
                )
                col_type = col_type or "str"

                if col_type not in ["int", "float", "str", "datetime"]:
                    raise ValueError("Invalid column type")
                col_type = cast(ColumnType, col_type)

                table.columns.append(
                    Column(
                        name=column,
                        type=col_type,
                    )
                )

            print(f"Importing data from {file_name}")
            rows = []
            for i, row in enumerate(reader):
                table.next_id = i
                imported: List[Any] = [i]

                for column in table.columns[1:]:
                    if column.type == "int":
                        imported.append(int(row[column.name]))
                    elif column.type == "float":
                        imported.append(float(row[column.name]))
                    elif column.type == "datetime":
                        imported.append(datetime.fromisoformat(row[column.name]))
                    elif column.type == "str":
                        imported.append(row[column.name])
                    else:
                        raise ValueError(
                            f"Invalid column type for import: {column.type}"
                        )

                rows.append(imported)

            table.save(
                ResultSet(
                    table_name=table_name,
                    columns=table.columns,
                    rows=rows,
                )
            )

        meta.database.tables.append(table)
        __save_meta(meta)


def __save_meta(meta: Metadata) -> None:
    with open(META_FILE, "w") as f:
        f.write(json.dumps(dataclasses.asdict(meta), indent=2))
        print(f"Metadata file saved to {META_FILE.as_posix()}")
