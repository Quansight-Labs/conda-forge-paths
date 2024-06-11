#!/bin/bash

set -euxo pipefail

curl -L -o path_to_artifacts.tar.zst https://github.com/jaimergp/conda-forge-paths/releases/latest/download/path_to_artifacts.tar.zst
curl -L -o path_to_artifacts.db.sha256 https://github.com/jaimergp/conda-forge-paths/releases/latest/download/path_to_artifacts.db.sha256
tar xf path_to_artifacts.tar.zst
rm path_to_artifacts.tar.zst

if [[ "$(openssl sha256 path_to_artifacts.db)" != "$(cat path_to_artifacts.db.sha256)" ]]; then
    echo "SHA256 mismatch! Won't update redeploy"
    exit 1
fi

mv path_to_artifacts.db path_to_artifacts.production.db
datasette serve -i "path_to_artifacts.production.db" -m datasette.yml
