#!/bin/bash

. ~/miniconda3/etc/profile.d/conda.sh
conda activate datasette

set -euxo pipefail

if [[ $1 == "update" ]]; then
    curl -sfL -o path_to_artifacts.tar.zst \
        https://github.com/Quansight-Labs/conda-forge-paths/releases/latest/download/path_to_artifacts.tar.zst
    curl -sfL -o path_to_artifacts.db.sha256 \
        https://github.com/Quansight-Labs/conda-forge-paths/releases/latest/download/path_to_artifacts.db.sha256
    mkdir -p extracted
    tar xf path_to_artifacts.tar.zst -C extracted
    rm path_to_artifacts.tar.zst

    if [[ "$(openssl sha256 extracted/path_to_artifacts.db | cut -d ' ' -f2)" != "$(cat path_to_artifacts.db.sha256  | cut -d ' ' -f2)" ]]; then
        echo "SHA256 mismatch! Won't update redeploy"
        exit 1
    fi
    mv extracted/path_to_artifacts.db path_to_artifacts.db

    curl -sfL -o datasette.update.yml \
        https://raw.githubusercontent.com/Quansight-Labs/conda-forge-paths/main/datasette.yml \
        && mv datasette.update.yml datasette.yml \
        || true
elif [[ $1 == "run" ]]; then
    export DATASETTE_SECRET=$(python -c 'import secrets; print(secrets.token_hex(32))')
    datasette serve \
        -i "path_to_artifacts.db" \
        -m datasette.yml \
        -p "$DATASETTE_PORT" \
        --setting allow_download off \
        --setting allow_csv_stream off \
        --setting max_csv_mb 10
else
    echo "Unrecognized task: $1"
    exit 1
fi
