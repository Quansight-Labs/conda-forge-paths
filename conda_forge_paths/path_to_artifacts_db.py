import bz2
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from itertools import batched, chain, product
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlretrieve

from conda_forge_metadata.artifact_info import get_artifact_info_as_json
from conda_forge_metadata.artifact_info.info_json import info_json_from_tar_generator
from conda_forge_metadata.repodata import SUBDIRS, all_labels
from conda_forge_metadata.streaming import get_streamed_artifact_data

try:
    from tqdm.auto import tqdm
except ImportError:

    def tqdm(iterator, *args, **kwargs):
        desc = kwargs.pop("desc", "")
        if desc:
            print(desc)
        return iterator


DBPATH = "path_to_artifacts.db"
log = logging.getLogger(__name__)


def connect(bootstrap=False):
    kwargs = {"isolation_level": None} if bootstrap else {}
    db = sqlite3.connect(DBPATH, **kwargs)
    if bootstrap:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS Artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact TEXT NOT NULL UNIQUE,
                timestamp INTEGER DEFAULT 0 NOT NULL
            );
            CREATE TABLE IF NOT EXISTS PathToArtifactIds (
                path TEXT PRIMARY KEY,
                basename TEXT,
                artifact_ids TEXT
            );
            CREATE TABLE IF NOT EXISTS LatestSuccessfulUpdate (
                id INTEGER PRIMARY KEY CHECK (id = 0),
                timestamp INTEGER DEFAULT 0 NOT NULL
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
                except Exception as exc:
                    log.exception("Error reading %s", path, exc_info=exc)
                    continue
                artifact = "/".join(["cf", path.parts[-2], path.stem])
                artifacts_timestamp.append(
                    (artifact, data.get("index", {}).get("timestamp", 0))
                )
                for path in data["files"]:
                    mapping.setdefault(path, []).append(artifact)
            yield (
                artifacts_timestamp,
                (
                    (path, os.path.basename(path), artifacts)
                    for path, artifacts in mapping.items()
                ),
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
            """
            SELECT artifact
            FROM Artifacts, PathToArtifactIds, json_each('[' || PathToArtifactIds.artifact_ids || ']') as each_id
            WHERE PathToArtifactIds.path = (?) AND each_id.value = Artifacts.id
            """,
            (q,),
        ):
            yield row


def most_recent_artifact(db) -> tuple[str, int]:
    for row in db.execute(
        """
        SELECT artifact, timestamp
        FROM Artifacts
        ORDER BY timestamp DESC
        LIMIT 1
        """
    ):
        return row


def get_latest_successful_update(db):
    try:
        for row in db.execute(
            "SELECT timestamp from LatestSuccessfulUpdate WHERE id = 0"
        ):
            return row[0]
    except sqlite3.OperationalError as exc:
        log.exception(exc)
        return 0


def set_latest_successful_update(db, timestamp: int | None = None):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS LatestSuccessfulUpdate (
            id INTEGER PRIMARY KEY CHECK (id = 0),
            timestamp INTEGER DEFAULT 0 NOT NULL
        );
        """
    )
    if timestamp is None:
        _, timestamp = most_recent_artifact(db)
    db.execute(
        "UPDATE LatestSuccessfulUpdate SET timestamp = (?) WHERE id = 0",
        (timestamp,),
    )


def count_artifacts(db):
    for row in db.execute(
        """
        SELECT COUNT(*)
        FROM Artifacts
        """
    ):
        return row[0]


def fetch_and_extract_one(url, dest):
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Download the file
    attempts = 0
    while attempts < 5:
        try:
            download_location, _ = urlretrieve(url)
        except HTTPError:
            time.sleep(1 * attempts)
            continue
        else:
            try:
                with open(download_location, "rb") as compressed, open(dest, "wb") as f:
                    f.write(bz2.decompress(compressed.read()))
                Path(download_location).unlink()
            except OSError:
                time.sleep(1 * attempts)
                continue
            else:
                break
    else:
        raise RuntimeError(f"Could not download or extract URL: {url}")


def fetch_repodata(
    subdirs=SUBDIRS,
    force_download=False,
    cache_dir=".repodata_cache",
    label="main",
):
    assert all(subdir in SUBDIRS for subdir in subdirs)
    paths = []
    for subdir in subdirs:
        prefix = "https://conda.anaconda.org/conda-forge"
        if label == "main":
            # We don't need patches, and this way we can get 'removed' items with timestamps
            repodata = f"{prefix}/{subdir}/repodata_from_packages.json"
        else:
            repodata = f"{prefix}/label/{label}/{subdir}/repodata.json"
        local_fn = Path(cache_dir, f"{subdir}.{label}.json")
        paths.append(local_fn)
        if force_download or not local_fn.exists():
            fetch_and_extract_one(repodata + ".bz2", local_fn)
    return paths


def new_artifacts(ts):
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for label, subdir in product(all_labels(use_remote_cache=True), SUBDIRS):
            future = executor.submit(
                fetch_repodata, (subdir,), False, ".repodata_cache", label
            )
            futures.append(future)
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Fetching repodata"
        ):
            try:
                repodatas = future.result()
            except Exception as exc:
                log.exception("! Failed to download some labels", exc_info=exc)
                continue
            for repodata in repodatas:
                subdir, label = repodata.stem.split(".", 1)
                if label == "main":
                    channel = "cf"
                else:
                    channel = f"cf-{label}"
                try:
                    data = json.loads(repodata.read_text())
                except Exception as exc:
                    log.exception("Error reading %s", repodata, exc_info=exc)
                    continue
                keys = {"packages": ".tar.bz2", "packages.conda": ".conda"}
                for key, ext in keys.items():
                    for pkg, pkg_data in data.get(key, {}).items():
                        timestamp = pkg_data.get("timestamp", 0)
                        if timestamp > ts:
                            yield (
                                f"{channel}/{subdir}/{pkg[:-len(ext)]}",
                                timestamp,
                                ext,
                            )

                for pkg in data.get("removed", ()):
                    ext = ".tar.bz2" if pkg.endswith(".tar.bz2") else ".conda"
                    yield (f"{channel}/{subdir}/{pkg[:-len(ext)]}", 0, ext)


def files_from_artifact(artifact):
    time.sleep(0.05)
    channel, subdir, artifact = artifact.rsplit("/", 2)
    if "-" in channel:
        channel, label = channel.split("-", 1)
        channel = "https://conda.anaconda.org/conda-forge/label/" + label
    else:
        channel = "conda-forge"

    if artifact.endswith(".conda"):
        # .conda artifacts can be streamed directly from an anaconda.org channel
        try:
            data = get_artifact_info_as_json(
                channel=channel,
                subdir=subdir,
                artifact=artifact,
                backend="streamed",
                skip_files_suffixes=(),
            )
            if data and data.get("name"):
                return data
        except Exception as exc:
            # Maybe we are lucky and the payload is in OCI :)
            log.exception("Skipping %s", artifact, exc_info=exc)

    # .tar.bz2 artifacts need to be downloaded and extracted, but the OCI mirror has
    # the info layer that we can use to get the files list
    data = get_artifact_info_as_json(
        channel=channel,
        subdir=subdir,
        artifact=artifact,
        backend="oci",
        skip_files_suffixes=(),
    )
    if data and data.get("name"):
        return data

    # Last resort, we download the tar.bz2 and hope is not too big.
    # This is mostly for .tar.bz2 artifacts in labels that are not OCI mirrored.
    try:
        data = info_json_from_tar_generator(
            get_streamed_artifact_data(channel, subdir, artifact),
            skip_files_suffixes=(),
        )
        if data and data.get("name"):
            return data
    except OSError as exc:
        # Try with non-CDN location; note this endpoint doesn't have HTTP range requests.
        # .conda files will fail this fallback.
        if channel == "conda-forge":
            channel = "https://conda-web.anaconda.org/conda-forge"
        else:
            channel = channel.replace("conda.anaconda.org", "conda-web.anaconda.org")
        data = info_json_from_tar_generator(
            get_streamed_artifact_data(channel, subdir, artifact),
            skip_files_suffixes=(),
        )
        if data and data.get("name"):
            return data
        raise RuntimeError(f"Could not fetch {artifact}") from exc


def update_from_repodata(db):
    """
    The artifacts table always stores all the filenames in the repodata.
    It serves as an inventory and also a todo list.

    On Artifacts updates, we gather the newly added rows and use those
    to query the actual info/ metadata remotely. These queries can fail
    due to network issues and whatnot, so we catch potential exceptions
    and delete those form the Artifacts table so they are retried eventually.
    """
    start_from = (
        get_latest_successful_update(db) or 1701843236881
    )  # Dec 2023 (last libcfgraph item)
    print(
        "Starting from",
        start_from / 1000,
        datetime.fromtimestamp(start_from / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S %Z"),
    )
    to_add, null_ts_artifacts = [], []
    for artifact, ts, ext in sorted(
        tqdm(new_artifacts(start_from), desc="Identifying artifacts to add"),
        key=lambda x: x[1],  # sort by timestamp
    ):
        if not ts:  # broken artifacts have ts = 0
            null_ts_artifacts.append((artifact, 0, ext))
        else:
            to_add.append((artifact, ts, ext))

    for batch in batched(
        tqdm(
            chain(to_add, null_ts_artifacts),
            desc="Adding artifacts",
            leave=False,
            total=len(to_add) + len(null_ts_artifacts),
        ),
        1000,
    ):
        ids = db.execute(
            """
            INSERT INTO Artifacts (artifact, timestamp) 
            VALUES {values}
                ON CONFLICT(artifact) DO NOTHING
            RETURNING *
            """.format(values=", ".join(f"('{name}', {ts})" for name, ts, _ in batch))
        )
        name_to_id = {name: id_ for id_, name, _ in ids}
        name_to_ext = {name: ext for (name, _, ext) in batch if name in name_to_id}
        files_to_artifact = {}
        failed_artifacts = []
        futures = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(files_from_artifact, name + name_to_ext[name]): name
                for name in name_to_id
            }
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Fetching files",
                leave=False,
                disable=os.environ.get("CI"),
            ):
                name = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    exc_id = ".".join(
                        [
                            getattr(exc.__class__, "__module__", ""),
                            exc.__class__.__name__,
                        ]
                    )
                    failed_artifacts.append((name, f"{exc_id}: {exc}"))
                    log.exception(exc)
                else:
                    if data is None:
                        failed_artifacts.append((name, "Empty metadata payload"))
                    else:
                        for f in data.get("files", ()):
                            files_to_artifact.setdefault(f, []).append(name)

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
                    os.path.basename(path),
                    ",".join([str(name_to_id[artifact]) for artifact in artifacts]),
                )
                for path, artifacts in files_to_artifact.items()
            ),
        )
        if failed_artifacts:
            # Remove from the Artifacts table so we retry at some point
            q = """
                DELETE FROM Artifacts
                WHERE artifact IN ({})
                """.format(", ".join(f"'{name}'" for name, _ in failed_artifacts))
            try:
                db.execute(q)
            except sqlite3.OperationalError as exc:
                log.error("Exception for query: %s", q)
                raise exc
            with open("failed_artifacts.txt", "a") as f:
                f.write("\n".join(map(str, failed_artifacts)))
                f.write("\n")
        db.commit()


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    if len(sys.argv) == 3:
        action = sys.argv[1]
        if action == "bootstrap":
            artifacts_dir = sys.argv[2]
            db = connect(bootstrap=True)
            bootstrap_from_libcfgraph_path_to_artifact(db, artifacts_dir)
            db.commit()
            db.close()
            sys.exit()

        if action in ("find-artifacts", "find-paths"):
            db = connect()
            t0 = time.time()
            for i, row in enumerate(query(db, sys.argv[2], fts=action == "find-paths")):
                print(f"{i}) {row[0]}")
            print(f"Query took {time.time() - t0:.4f} seconds")
            db.close()
            sys.exit()

    if len(sys.argv) == 2:
        if sys.argv[1] == "fts":
            db = connect()
            t0 = time.time()
            index_full_text_search(db)
            print(f"FTS indexing took {time.time() - t0:.4f} seconds")
            db.close()
            sys.exit()

        if sys.argv[1] == "most-recent-artifact":
            db = connect()
            name, ts = most_recent_artifact(db)
            print(
                name,
                ts / 1000,
                datetime.fromtimestamp(ts / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S %Z"),
            )
            db.close()
            sys.exit()

        if sys.argv[1] == "most-recent-successful-update":
            db = connect()
            ts = get_latest_successful_update(db)
            print(
                ts / 1000,
                datetime.fromtimestamp(ts / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S %Z"),
            )
            db.close()
            sys.exit()

        if sys.argv[1] == "update-from-repodata":
            db = connect()
            print("Artifacts before update:", count_artifacts(db))
            update_from_repodata(db)
            print("Artifacts after update:", count_artifacts(db))
            name, ts = most_recent_artifact(db)
            print(
                "Most recent one:",
                name,
                ts / 1000,
                datetime.fromtimestamp(ts / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S %Z"),
            )
            failed = Path("failed_artifacts.txt")
            if failed.is_file():
                log.warning("Couldn't fetch these artifacts, please retry:")
                with open(failed) as f:
                    for i, line in enumerate(f, 1):
                        log.warning("%s. %s", i, line)
            else:
                # Update epoch timestamp because no errors happened :D
                set_latest_successful_update(db)
            db.close()
            sys.exit()

    print(
        f"Usage: {sys.argv[0]} subcommand",
        "subcommands:",
        "  - bootstrap /path/to/libcfgraph/artifacts/  # initialize the database",
        "  - fts                                       # index the full text search",
        "  - find-artifacts <full path>                # find artifacts by full path",
        "  - find-paths <path component>               # find full paths by partial matches",
        "  - update-from-repodata                      # update the database from current repodata",
        "  - most-recent-artifact                      # print latest artifact in database",
        sep="\n",
    )
    sys.exit(1)
