#!/bin/bash

[ $# -ne 1 ] && { echo "Usage: $0 kb_dir"; exit 1; }
kb_dir=$(readlink -f $1)

cd $(dirname $0)

source activate xy_linking
set -x
python linking.py --index $kb_dir
