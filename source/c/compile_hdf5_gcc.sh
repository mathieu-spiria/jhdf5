#! /bin/bash

source version.sh
PLATFORM="$1"
PATCHES="$2"

if [ "$PLATFORM" != "i386" -a "$PLATFORM" != "x86" -a "$PLATFORM" != "amd64" -a "$PLATFORM" != "x86_64" -a "$PLATFORM" != "armv6l" -a "$PLATFORM" != "aarch64" ]; then
  echo "Syntax: compile_hdf5.sh <platform>"
  echo "where <platform> is one of i386, x86, amd64, x86_64, aarch64, or armv6l"
  exit 1
fi

rm -fR build
mkdir build

cd build
BUILD_ROOT=`pwd`

tar xvf ../hdf5-$VERSION.tar*

if [ -n "$POSTFIX" ]; then
  mv hdf5-$VERSION hdf5-$VERSION-$POSTFIX
  VERSION="$VERSION-$POSTFIX"
fi

cd hdf5-$VERSION

if [ -n "$PATCHES" ]; then
  for p in $PATCHES; do
    patch -p1 < ../../$p
  done
fi

CFLAGS=$CFLAGS ./configure --prefix=$BUILD_ROOT/hdf5-$VERSION-$PLATFORM --enable-build-mode=production $ADDITIONAL &> configure.log

if [ "`uname`" == "Darwin" ]; then
   NCPU=`sysctl -n hw.ncpu`
else
   NCPU=`lscpu|awk '/^CPU\(s\)/ {print $2}'`
fi

make -j $NCPU &> build.log
make install &> install.log
make test &> test.log
