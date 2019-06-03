#!/bin/bash -x

cd $(dirname $0)

[ -e "$JAVA_HOME" ] || { echo "ERROR: Please set JAVA_HOME correctly!"; exit 1; }

conda create -n xy_linking python=2.7.15
source activate xy_linking

wget -q https://www-us.apache.org/dist/lucene/pylucene/pylucene-7.6.0-src.tar.gz
tar xzf pylucene-7.6.0-src.tar.gz

cd pylucene-7.6.0/jcc/
python setup.py build && python setup.py install
cd ..
make all install \
    PYTHON=$(which python) \
    JCC="$(which python) -m jcc --shared" \
    ANT="JAVA_HOME=$JAVA_HOME /usr/bin/ant" \
    NUM_FILES=8
source deactivate
