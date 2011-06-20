#! /bin/bash

source version.sh

gcc -m32 -shared -O3 jhdf5/*.c hdf-java/*.c -I/opt/hdf5-${VERSION}-i386/include -I/usr/java/jdk1.5.0_16/include -I/usr/java/jdk1.5.0_16/include/linux /opt/hdf5-${VERSION}-i386/lib/libhdf5.a -o libjhdf5.so -lz
