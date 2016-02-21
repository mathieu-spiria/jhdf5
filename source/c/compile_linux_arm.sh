#! /bin/bash

source version.sh

if [ -n "$POSTFIX" ]; then
  VERSION="$VERSION-$POSTFIX"
fi

gcc -shared -O3 -Wl,--exclude-libs,ALL jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-armv6l/include -I/usr/java/jdk1.7.0/include -I/usr/java/jdk1.7.0/include/linux /opt/hdf5-${VERSION}-armv6l/lib/libhdf5.a -o libjhdf5.so -lz
