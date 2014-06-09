#! /bin/bash

source version.sh

gcc -shared -O3 -floop-interchange -floop-strip-mine -floop-block -fgraphite-identity -mtune=corei7 -fPIC -Wl,--exclude-libs,ALL jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-amd64/include -I/usr/java/jdk1.6.0/include -I/usr/java/jdk1.6.0/include/linux /opt/hdf5-${VERSION}-amd64/lib/libhdf5.a -o libjhdf5.so -lz
