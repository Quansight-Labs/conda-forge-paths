"""
Bootstrap a path-to-artifact sqlite database
using regro/libcfgraph's JSON artifacts.

The script expects the path to the artifacts/ directory as an argument.

This is a one-time operation that takes around 10 minutes.
"""

import json
import os
import sqlite3
import sys
from itertools import batched
from pathlib import Path

from tqdm.auto import tqdm


def connect(bootstrap=False):
    kwargs = {"isolation_level": None} if bootstrap else {}
    db = sqlite3.connect("path_to_artifacts.db", **kwargs)
    if bootstrap:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS PathToArtifacts (
                path TEXT PRIMARY KEY,
                basename TEXT,
                artifacts TEXT
            );
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = 0;
            PRAGMA cache_size = 1000000;
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
            """
        )
    return db


def bootstrap_from_libcfgraph_path_to_artifact(db, artifacts_dir):
    def iterator():
        for batch in tqdm(
            batched(Path(artifacts_dir).glob("**/*.json"), 1000),
            total=1_602_023 // 1000,  # we know this number from previous experiments :)
        ):
            mapping = {}
            for path in batch:
                try:
                    data = json.loads(path.read_text())
                except Exception as e:
                    print(f"Error reading {path}: {e}")
                    continue
                artifact = "/".join([*path.parts[-3:-1], path.stem])
                for path in data["files"]:
                    mapping.setdefault(path, []).append(artifact)
            yield (
                (path, os.path.basename(path), "\n".join(artifacts))
                for path, artifacts in mapping.items()
            )

    db.execute("BEGIN")
    for i, batch in enumerate(iterator()):
        db.executemany(
            """
            INSERT INTO PathToArtifacts (path, basename, artifacts) 
                VALUES (?, ?, ?)
            ON CONFLICT(path) DO 
                UPDATE SET artifacts = artifacts || '\n' || excluded.artifacts
            """,
            batch,
        )
        if i % 100 == 0:
            db.commit()
            db.execute("BEGIN")


def query(db, q, limit=10):
    field = "path" if "/" in q else "basename"
    for row in db.execute(
        f"""
        SELECT path, artifacts
        FROM PathToArtifacts
        WHERE {field} = (?)
        LIMIT {limit}
        """,
        (q,),
    ):
        yield row


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            f"Usage: {sys.argv[0]} [bootstrap /path/to/libcfgraph/artifacts/] | [query <path>]"
        )
        sys.exit(1)
    action = sys.argv[1]
    if action == "bootstrap":
        artifacts_dir = sys.argv[2]
        db = connect(bootstrap=True)
        bootstrap_from_libcfgraph_path_to_artifact(db, artifacts_dir)
        db.commit()
        db.close()
    elif action == "query":
        db = connect()
        for row in query(db, sys.argv[2]):
            print(row[0], "\n -", "\n - ".join(row[1].split("\n")))
        db.close()
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)
