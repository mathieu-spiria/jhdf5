#! /bin/bash

CFLAGS='-m32' ./compile_hdf5_gcc.sh i386 "HDFFV-9670-1.8.16.patch gcc-4+-optimizations-config.patch"
