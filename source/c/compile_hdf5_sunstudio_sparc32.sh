#! /bin/bash

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH

source version.sh

tar xf hdf5-$VERSION.tar

cd hdf5-$VERSION

CPPFLAGS='-D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE' ./configure --prefix=/opt/hdf5-$VERSION-32 --enable-shared --enable-debug=none 

make > make.log 2>&1

make test > test.log 2>&1
