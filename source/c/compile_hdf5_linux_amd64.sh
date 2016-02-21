#! /bin/bash

CFLAGS='-fPIC -m64' ./compile_hdf5_gcc.sh amd64 "HDFFV-9670-1.8.16.patch gcc-4+-optimizations-config.patch"
