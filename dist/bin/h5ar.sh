#! /bin/bash

# This script requires the the readlink binary. If your system lacks this binary, $JHDFDIR needs to be hard-coded

SCRIPT="$0"
BINDIR="${SCRIPT%/*}"
LINK="`readlink $0`"
while [ -n "${LINK}" ]; do
  if [ "${LINK#/}" = "${LINK}" ]; then
    SCRIPT="${BINDIR}/${LINK}"
  else
    SCRIPT="${LINK}"
  fi
  LINK="`readlink ${SCRIPT}`"
done
BINDIR="${SCRIPT%/*}"
JHDFDIR="${BINDIR%/*}"
java -Dnative.libpath="${JHDFDIR}/lib/native" -jar "${JHDFDIR}/lib/cisd-jhdf5-tools.jar" "$@"
