#!/bin/bash

[ $# -lt 2 ] && { echo "Usage: $0 kb_dir index_dir [country_code1] [country_code2] ..."; exit 1; }
kb_dir=$(readlink -f $1)
index_dir=$(readlink -f $2)

cd $(dirname $0)

source activate xy_linking
set -x
if [ $# -eq 2 ]; then
    python linking.py --index $kb_dir --index-dir $index_dir
else
    shift 2
    python linking.py --index $kb_dir --index-dir $index_dir --country-codes "${@}"
fi
