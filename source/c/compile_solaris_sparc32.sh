#! /bin/bash

source version.sh

if [ -n "$POSTFIX" ]; then
  VERSION="$VERSION-$POSTFIX"
fi

cc -G -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-32/include -I/usr/java/include -I/usr/java/include/solaris /opt/hdf5-${VERSION}-32/lib/libhdf5.a -lz -o libjhdf5.so
