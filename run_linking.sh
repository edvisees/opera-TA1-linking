#!/bin/bash

[ $# -ne 3 ] && { echo "Usage: $0 csr_dir out_dir en|ru|uk|img"; exit 1; }

csr_dir=$(readlink -f $1)
out_dir=$(readlink -f $2)
lang=$3

cd $(dirname $0)

source activate xy_linking
set -x
python linking.py --run_csr --$lang --in_dir $csr_dir --out_dir $out_dir
exit $?
