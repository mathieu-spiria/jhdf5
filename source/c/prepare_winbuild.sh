#! /bin/bash

source version.sh

rm -fR build
mkdir build

cd build
BUILD_ROOT=`pwd`

unzip ../CMake-hdf5-$VERSION.zip

if [ -n "$POSTFIX" ]; then
  mv CMake-hdf5-$VERSION CMake-hdf5-$VERSION-$POSTFIX
fi

rm -fR CMake-hdf5-$VERSION/hdf5-$VERSION/java/src/jni
cp -af ../jni CMake-hdf5-$VERSION/hdf5-$VERSION/java/src/
cp -af ../*.c CMake-hdf5-$VERSION/hdf5-$VERSION/java/src/jni/

cd CMake-hdf5-$VERSION
patch -p1 < ../../cmake_set_hdf5_options.diff

cd hdf5-$VERSION
patch -p2 < ../../../cmake_add_sources.diff
