#!/bin/bash

[ $# -lt 2 ] && { echo "Usage: $0 kb_dir index_dir [country_code1] [country_code2] ..."; exit 1; }
kb_dir=$(readlink -f $1)
index_dir=$(readlink -f $2)

cd $(dirname $0)

source activate xy_linking
set -x
if [ -d $index_dir/lucene_index/ ]; then
    mkdir -p $index_dir/extra_kb_entries/data/
    cp -a $kb_dir/data/alternate_names.tab $index_dir/extra_kb_entries/data/
    LC_ALL=C grep -v -e '^GEO' -e '^WLL' -e '^APB' $kb_dir/data/entities.tab > $index_dir/extra_kb_entries/data/entities.tab
    kb_dir=$index_dir/extra_kb_entries/
fi

shift 2
python linking.py --index $kb_dir --index-dir $index_dir --country-codes "${@}"
