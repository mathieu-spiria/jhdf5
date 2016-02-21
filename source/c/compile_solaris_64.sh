#! /bin/bash

source version.sh

if [ -n "$POSTFIX" ]; then
  VERSION="$VERSION-$POSTFIX"
fi
cc -G -KPIC -fast -m64 jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-64/include -I/usr/java/include -I/usr/java/include/solaris /opt/hdf5-${VERSION}-64/lib/libhdf5.a -lz -o libjhdf5.so
