cc -G -KPIC -fast -xarch=amd64 *.c -I/opt/hdf5-1.8.1-amd64/include -I/usr/java/include -I/usr/java/include/solaris /opt/hdf5-1.8.1-amd64/lib/libhdf5.a -lz -o jhdf5.so
