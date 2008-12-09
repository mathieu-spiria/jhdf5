#! /bin/sh

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH
CPPFLAGS='-D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE' ./configure --prefix=/opt/hdf5-1.8.2-32 --enable-shared --enable-debug=none && make
