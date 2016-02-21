#! /bin/bash

source version.sh
PLATFORM="$1"
PATCHES="$2"

if [ "$PLATFORM" != "i386" -a "$PLATFORM" != "x86" -a "$PLATFORM" != "amd64" -a "$PLATFORM" != "x86_64" -a "$PLATFORM" != "armv6l" ]; then
  echo "Syntax: compile_hdf5.sh <platform>"
  echo "where <platform> is one of i386, x86, amd64, x86_64, or armv6l"
  exit 1
fi

tar xvf hdf5-$VERSION.tar

if [ -n "$POSTFIX" ]; then
  mv hdf5-$VERSION hdf5-$VERSION-$POSTFIX
  VERSION="$VERSION-$POSTFIX"
fi

cd hdf5-$VERSION

if [ -n "$PATCHES" ]; then
  for p in $PATCHES; do
    patch -p1 < ../$p
  done
fi

CFLAGS=$CFLAGS ./configure --prefix=/opt/hdf5-$VERSION-$PLATFORM --enable-debug=none --enable-production $ADDITIONAL &> configure.log

make &> make.log

make test &> test.log
