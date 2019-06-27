#!/bin/bash -x

cd $(dirname $0)

export JAVA_HOME=${JAVA_HOME:-"/usr/lib/jvm/java-8-openjdk-amd64"}
[ -e "$JAVA_HOME" ] || { echo "ERROR: Please set JAVA_HOME correctly!"; exit 1; }

conda create -n xy_linking python=2.7.15
source activate xy_linking

if [ -d "../../resources/xianyang/xianyang_linking/pylucene-7.6.0" ]; then
    cd ../../resources/xianyang/xianyang_linking/pylucene-7.6.0/jcc/
else
    wget -q https://www-us.apache.org/dist/lucene/pylucene/pylucene-7.6.0-src.tar.gz
    tar xzf pylucene-7.6.0-src.tar.gz
    cd pylucene-7.6.0/jcc/
fi
export JCC_JDK=$JAVA_HOME
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64:$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib:$LD_LIBRARY_PATH
python setup.py clean && python setup.py build && python setup.py install
cd ..
make clean all install \
    PYTHON=$(which python) \
    JCC="$(which python) -m jcc --shared" \
    ANT="JAVA_HOME=$JAVA_HOME /usr/bin/ant" \
    NUM_FILES=8
source deactivate
