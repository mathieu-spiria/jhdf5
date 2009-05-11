#! /bin/sh

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH

VERSION=1.8.3

tar xf hdf5-$VERSION.tar

cd hdf5-$VERSION

CFLAGS='-fast -m64 -KPIC' ./configure --prefix=/opt/hdf5-1.8.3-64 --enable-shared --enable-debug=none 

make &> make.log

make test &> test.log
