import os
from pathlib import Path


DATA_DIR = Path(os.path.dirname(__file__)) / "db_data"
META_FILE = DATA_DIR / "meta.json"
