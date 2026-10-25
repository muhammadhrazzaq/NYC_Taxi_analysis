#!/usr/bin/env bash
set -euo pipefail
gh release download data-v1 -p data.zip --clobber
unzip -o data.zip -d data/raw
rm data.zip
echo "Data ready in data/raw/"