#! /bin/sh

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH

VERSION=1.8.4

tar xf hdf5-$VERSION.tar

cd hdf5-$VERSION

CFLAGS='-fast -m64 -KPIC' ./configure --prefix=/opt/hdf5-$VERSION-64 --enable-shared --enable-debug=none 

make > make.log 2>&1

make test > test.log 2>&1
