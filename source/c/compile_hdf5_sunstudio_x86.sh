#! /bin/sh

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH

VERSION=1.8.3

tar xf hdf5-$VERSION.tar

cd hdf5-$VERSION

CPPFLAGS='-D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE' CFLAGS='-KPIC' ./configure --prefix=/opt/hdf5-1.8.3-32 --enable-shared --enable-debug=none 

make &> make.log

make test &> test.log
