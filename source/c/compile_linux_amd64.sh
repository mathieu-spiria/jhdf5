gcc -shared -O3 -fPIC *.c -I/opt/hdf5-1.8.2-amd64/include -I/usr/java/jdk1.5.0_16/include -I/usr/java/jdk1.5.0_16/include/linux /opt/hdf5-1.8.2-amd64/lib/libhdf5.a -o libjhdf5.so -lz
