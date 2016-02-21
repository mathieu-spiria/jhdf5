#! /bin/bash

source version.sh

rm -f hdf5-$VERSION-win.zip
rm -fR hdf5-$VERSION
tar xvf hdf5-$VERSION.tar
if [ -n "$POSTFIX" ]; then
  mv hdf5-$VERSION hdf5-$VERSION-$POSTFIX
  VERSION="$VERSION-$POSTFIX"
fi
cd hdf5-$VERSION

patch -p1 < ../HDFFV-9670-1.8.16.patch
patch -p0 < ../hdf5_win_compile.diff
find . -name "*.orig" -exec rm {} \;

cp -f config/cmake/UserMacros/Windows_MT.cmake UserMacros.cmake
fgres "option (BUILD_STATIC_CRT_LIBS \"Build With Static CRT Libraries\" OFF)" "option (BUILD_STATIC_CRT_LIBS \"Build With Static CRT Libraries\" ON)" UserMacros.cmake
grep -q -F 'option (BUILD_STATIC_CRT_LIBS "Build With Static CRT Libraries" ON)' UserMacros.cmake || echo "Patching UserMacros.cmake for static build FAILED"
cd ..

zip -rq hdf5-$VERSION-win.zip hdf5-$VERSION
rm -fR hdf5-$VERSION
