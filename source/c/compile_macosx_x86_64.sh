#! /bin/bash

source version.sh

gcc -m64 -mmacosx-version-min=10.6 -dynamiclib -O3 jhdf5/*.c hdf-java/*.c -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers -I/opt/hdf5-${VERSION}-x86_64/include /opt/hdf5-${VERSION}-x86_64/lib/libhdf5.a -lz -o libjhdf5.jnilib
nm libjhdf5.jnilib | awk '/T _Java_/ { print $3 }' > JHDF5_interface_list
strip -u -r -s JHDF5_interface_list libjhdf5.jnilib
rm -f JHDF5_interface_list

