#! /bin/sh

PATH=/opt/SUNWspro/bin:/usr/local/bin:/opt/csw/bin:/usr/sbin:/usr/bin:/usr/openwin/bin:/usr/ccs/bin:/usr/ucb
export PATH
CFLAGS='-fast -m64 -KPIC' ./configure --prefix=/opt/hdf5-1.8.2-64 --enable-shared --enable-debug=none && make
