#! /bin/bash

SCRIPT="$0"
if [ "${SCRIPT}" = "${SCRIPT#/}" ]; then
  SCRIPT="`pwd`/${SCRIPT#./}"
fi
BINDIR="${SCRIPT%/*}"
BASEDIR=${BINDIR%/*}
cd $BASEDIR
ant -lib ../build_resources/lib/ecj.jar -f build/build.xml "$@"
