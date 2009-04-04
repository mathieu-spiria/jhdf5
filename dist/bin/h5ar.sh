#! /bin/bash

# This script requires the the readlink binary. If your system lacks this binary, $JHDFDIR needs to be hard-coded

SCRIPT="$0"
LINK="`readlink $0`"
while [ -n "$LINK" ]; do
  SCRIPT="${LINK}"
  LINK="`readlink ${SCRIPT}`"
done
BINDIR="${SCRIPT%/*}"
JHDFDIR="${BINDIR%/*}"

java -Dnative.libpath="${JHDFDIR}/lib/native" -jar "${JHDFDIR}/lib/jhdf5-tools.jar" "$@"
