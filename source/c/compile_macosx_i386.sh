#! /bin/bash

source version.sh

gcc -m32 -mmacosx-version-min=10.6 -bundle -O3 jhdf5/*.c hdf-java/*.c -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers -I/opt/hdf5-${VERSION}-i386/include /opt/hdf5-${VERSION}-i386/lib/libhdf5.a -lz -o libjhdf5.jnilib
