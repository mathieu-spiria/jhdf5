#! /bin/bash

source version.sh

rm -f hdf5-$VERSION-win.zip
rm -fR hdf5-$VERSION
tar xf hdf5-$VERSION.tar
cd hdf5-$VERSION

patch -s -p0 < ../hdf5_win_mt.diff
patch -s -p0 < ../hdf5_win_compile.diff
find . -name "*.orig" -exec rm {} \;

cp -f config/cmake/UserMacros/Windows_MT.cmake UserMacros.cmake
fgres "OPTION (BUILD_STATIC_CRT_LIBS \"Build With Static CRT Libraries\" OFF)" "OPTION (BUILD_STATIC_CRT_LIBS \"Build With Static CRT Libraries\" ON)" UserMacros.cmake
grep -q -F 'OPTION (BUILD_STATIC_CRT_LIBS "Build With Static CRT Libraries" ON)' UserMacros.cmake || echo "Patching UserMacros.cmake for static build FAILED"
cd ..

zip -rq hdf5-$VERSION-win.zip hdf5-$VERSION
rm -fR hdf5-$VERSION
