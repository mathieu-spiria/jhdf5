#! /bin/bash

source version.sh

gcc -m32 -shared -O3 -floop-interchange -floop-strip-mine -floop-block -fgraphite-identity -fuse-linker-plugin -flto -mtune=corei7 jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-i386/include -I/usr/java/jdk1.6.0_37/include -I/usr/java/jdk1.6.0_37/include/linux /opt/hdf5-${VERSION}-i386/lib/libhdf5.a -o libjhdf5.so -lz
