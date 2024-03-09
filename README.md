# conda-forge-paths

Find which conda package provides a given file path

## Bootstrap database

The main script is `conda_forge_paths/path_to_artifacts_db.py`. This is the last iteration of the
other underscore-leading script. It has a few subcommands we can use to bootstrap and query
the conda-forge path metadata.

To bootstrap, you need a local copy of the [`libcfgraph` repository](https://github.com/regro/libcfgraph):

```bash
# Initialize the database; takes ~10-15min
$ python conda_forge_paths/path_to_artifacts_db.py bootstrap path/to/libcfgraph-repo/artifacts
# Enable full text search; takes ~2min
$ python conda_forge_paths/path_to_artifacts_db.py fts
```

This should create a ~9GB `path_to_artifacts.db` file. It compresses nicely with `zstd`:

```bash
$ ZSTD_NBTHREADS=4 ZSTD_CLEVEL=19 tar --zstd -cf path_to_artifacts.tar.zst path_to_artifacts.db
```

## Queries

The script also has a couple of `find-*` subcommands:

```bash
# Find artifacts providing this exact file
$ python conda_forge_paths/path_to_artifacts_db.py find-artifacts 'bin/python'
# Find full paths given a path component
$ python conda_forge_paths/path_to_artifacts_db.py find-paths 'python'
```

The most recent artifact can be found with:

```bash
$ python conda_forge_paths/path_to_artifacts_db.py most-recent-artifact
# returns: cf/linux-64/llama-cpp-python-0.2.20-cuda112_habc0a91_2 1701711069.438 2023-12-04 17:31:09 UTC
```

This repo is also preconfigured for a datasette deployment, which offers the same query functionality:

```
$ datasette serve -i path_to_artifacts.db -m datasette.yml
```
