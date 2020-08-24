#!/bin/bash

# lucene_index_dir/ should contain subdir lucene_index/ and optionally tmp_index/
[ $# -lt 1 ] && { echo "Usage: $0 lucene_index_dir/ [country_code1] [country_code2] ..."; exit 1; }
index_dir=$(readlink -ve $1) || exit 1

cd $(dirname $0)

source activate xy_linking
set -x

shift 1
python linking.py --query --index-dir $index_dir --country-codes "${@}"
