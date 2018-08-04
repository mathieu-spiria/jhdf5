#! /bin/bash

CFLAGS='-fPIC -m64' ./compile_hdf5_gcc.sh amd64 "hdf5-1.10.3-pre1-jhdf5-h5dImp.patch hdf5-1.10.3-pre1-jhdf5-exceptionImp.patch"
