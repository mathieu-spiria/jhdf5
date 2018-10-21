#! /bin/bash

CFLAGS='-m64 -mmacosx-version-min=10.11' ./compile_hdf5_gcc.sh x86_64 "hdf5-1.10.4-jhdf5-h5dImp.patch"
