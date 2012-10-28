#! /bin/bash

CFLAGS='-fPIC -m64' ./compile_hdf5_gcc.sh amd64 "gcc-4.678-optimizations-config.patch"
