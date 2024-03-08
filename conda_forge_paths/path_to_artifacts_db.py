import json
import os
import sqlite3
import sys
import time
from itertools import batched
from pathlib import Path

from tqdm.auto import tqdm

DBPATH = "path_to_artifacts.db"


def connect(bootstrap=False):
    kwargs = {"isolation_level": None} if bootstrap else {}
    db = sqlite3.connect(DBPATH, **kwargs)
    if bootstrap:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS Artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact TEXT,
                timestamp INTEGER
            );
            CREATE TABLE IF NOT EXISTS PathToArtifactIds (
                path TEXT PRIMARY KEY,
                basename TEXT,
                artifact_ids TEXT
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
            artifacts_timestamp = []
            for path in batch:
                try:
                    data = json.loads(path.read_text())
                except Exception as e:
                    print(f"Error reading {path}: {e}")
                    continue
                artifact = "/".join(["cf", path.parts[-2], path.stem])
                artifacts_timestamp.append(
                    (artifact, data.get("index", {}).get("timestamp", 0))
                )
                for path in data["files"]:
                    mapping.setdefault(path, []).append(artifact)
            yield artifacts_timestamp, (
                (path, os.path.basename(path), artifacts)
                for path, artifacts in mapping.items()
            )

    db.execute("BEGIN")
    for i, (artifacts_timestamp, path_to_artifacts_iterator) in enumerate(iterator()):
        ids = db.execute(
            """
            INSERT INTO Artifacts (artifact, timestamp) 
                VALUES {values}
            RETURNING id;
            """.format(
                values=", ".join(
                    f"('{name}', {ts})" for name, ts in artifacts_timestamp
                )
            )
        )
        name_to_id = {
            name_ts[0]: id_
            for name_ts, subids in zip(artifacts_timestamp, ids)
            for id_ in subids
        }
        db.executemany(
            """
            INSERT INTO PathToArtifactIds (path, basename, artifact_ids) 
                VALUES (?, ?, ?)
            ON CONFLICT(path) DO 
                UPDATE SET artifact_ids = artifact_ids || ',' || excluded.artifact_ids
            """,
            (
                (
                    path,
                    basename,
                    ",".join([str(name_to_id[artifact]) for artifact in artifacts]),
                )
                for (path, basename, artifacts) in path_to_artifacts_iterator
            ),
        )
        if i % 1000 == 0:
            db.commit()
            db.execute("BEGIN")


def index_full_text_search(db):
    db.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS PathToArtifactIds_fts 
        USING fts5(
            path,
            tokenize="unicode61 tokenchars '_-.()[]?!+ 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' separators '/.' remove_diacritics 1",
            content=PathToArtifactIds
        );
        INSERT INTO PathToArtifactIds_fts(PathToArtifactIds_fts) VALUES('rebuild');
        """
    )
    db.commit()


def query(db, q, limit=100, fts=False):
    if (
        '"' in q
        or "'" in q
        or ";" in q
        or "--" in q
        or "/*" in q
        or "*/" in q
        or "," in q
    ):
        raise ValueError("Illegal query")
    if fts:
        for row in db.execute(
            f"""
            SELECT highlight(PathToArtifactIds_fts, 0, '*', '*')
            FROM PathToArtifactIds_fts
            WHERE PathToArtifactIds_fts MATCH '"{q}"'
            ORDER BY bm25(PathToArtifactIds_fts)
            LIMIT {limit}
            """
        ):
            yield row
    else:
        for row in db.execute(
            f"""
            SELECT group_concat(artifact, x'0a') 
            FROM Artifacts, PathToArtifactIds, json_each('[' || PathToArtifactIds.artifact_ids || ']') as each_id
            WHERE PathToArtifactIds.path = (?) AND each_id.value = Artifacts.id
            LIMIT {limit}
            """,
            (q,),
        ):
            yield row


if __name__ == "__main__":
    if len(sys.argv) == 3:
        action = sys.argv[1]
        if action == "bootstrap":
            artifacts_dir = sys.argv[2]
            db = connect(bootstrap=True)
            bootstrap_from_libcfgraph_path_to_artifact(db, artifacts_dir)
            db.commit()
            db.close()
        elif action in ("find-artifacts", "find-paths"):
            db = connect()
            t0 = time.time()
            for row in query(db, sys.argv[2], fts=action == "find-paths"):
                print(*row, sep="\n")
            print(f"Query took {time.time() - t0:.4f} seconds")
            db.close()
    elif len(sys.argv) == 2 and sys.argv[1] == "fts":
        db = connect()
        t0 = time.time()
        index_full_text_search(db)
        print(f"FTS indexing took {time.time() - t0:.4f} seconds")
        db.close()
    else:
        print(
            f"Usage: {sys.argv[0]} subcommand",
            "subcommands:" ,
            "  - bootstrap /path/to/libcfgraph/artifacts/  # initialize the database",
            "  - fts                                       # index the full text search",
            "  - [find-artifacts <full path>               # find artifacts by full path",
            "  - find-paths <path component>               # find full paths by partial matches",
            sep="\n"
        )
        sys.exit(1)
