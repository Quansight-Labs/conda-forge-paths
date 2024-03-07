"""
Bootstrap an artifact-to-json-blob sqlite database
using regro/libcfgraph's JSON artifacts.

The script expects the path to the artifacts/ directory as an argument.

This is a one-time operation that takes around 10 minutes.
"""

import sqlite3
import sys
import time
from itertools import batched
from pathlib import Path

from tqdm.auto import tqdm


def create_db():
    db = sqlite3.connect("artifact_to_json.db", isolation_level=None)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS ArtifactToJson (
            artifact TEXT PRIMARY KEY,
            data TEXT
        );
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = 0;
        PRAGMA cache_size = 1000000;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA temp_store = MEMORY;
        """
    )
    return db


def bootstrap_from_libcfgraph_artifact_to_json(db, artifacts_dir):
    def iterator():
        for path in Path(artifacts_dir).glob("**/*.json"):
            try:
                text = path.read_bytes()
            except Exception as e:
                print(f"Error reading {path}: {e}")
                continue
            artifact = "/".join([*path.parts[-3:-1], path.stem])
            yield (artifact, text)

    db = create_db()
    for batch in tqdm(batched(iterator(), 10000), total=1602023 // 10000):
        db.executemany(
            """
            INSERT OR IGNORE INTO ArtifactToJson (artifact, data)
            VALUES (?, jsonb(?))
            """,
            batch,
        )
    return db


def query(db, q):
    t0 = time.time()
    for row in db.execute(
        """
        SELECT artifact
        FROM ArtifactToJson, json_each(data, "$.files")
        WHERE json_each.value = (?)
        LIMIT 10
        """, 
        (q,)
    ):
        print("-", row)
    print(f"Query took {time.time() - t0:.2f} seconds")


if __name__ == "__main__":
    db = create_db()
    bootstrap_from_libcfgraph_artifact_to_json(db, sys.argv[1])
    db.commit()
    db.close()
