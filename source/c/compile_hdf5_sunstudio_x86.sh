#! /bin/bash

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH

source version.sh

tar xvf hdf5-$VERSION.tar

if [ -n "$POSTFIX" ]; then
  mv hdf5-$VERSION hdf5-$VERSION-$POSTFIX
  VERSION="$VERSION-$POSTFIX"
fi

cd hdf5-$VERSION

patch -p1 < ../HDFFV-9670-1.8.16.patch

CPPFLAGS='-D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE' CFLAGS='-KPIC' ./configure --prefix=/opt/hdf5-$VERSION-32 --enable-shared --enable-debug=none --enable-production

make > make.log 2>&1

make test > test.log 2>&1
