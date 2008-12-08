#============================================================================
#
#              Makefile to compile HDF Java Native C Source
#              Usage: nmake /f compile_windows_i386.mak
#
#============================================================================

# Visual C++ directory, for example
VCPPDIR=C:\Program Files\Microsoft Visual Studio 8\VC

# Directory where JDK is installed (We require JDK 1.5 or above), for example
JAVADIR=C:\Program Files\Java\jdk1.5.0_16

# Common parent directory
HDFPARENTDIR=C:\JHDF5

# Directory of the HDF Java Products, for example
HDFJAVADIR=$(HDFPARENTDIR)\jhdf5_src\

# The directory where HDF5 has been compiled
HDFDIR=$(HDFPARENTDIR)\hdf5\hdf5-1.8.2

# The directory where HDF library is located
HDFLIBDIR=$(HDFDIR)\proj\hdf5\Release

# The directory where HDF header files are located
HDFINCDIR=$(HDFDIR)\src

# the JPEG library, for example
#JPEGLIB=E:\Work\MyHDFstuff\lib-external\jpeg\libjpeg.lib
JPEGLIB=

# the GZIP library, for example
#GZIPLIB=E:\Work\MyHDFstuff\lib-external\zlib\bin\windows\zlib114\lib\zlib.lib
GZIPLIB=C:\JHDF5\hdf5\zlib123-x64-vs2005\lib\zlib.lib

# SZIP library, for example
#SZIPLIB=E:\Work\MyHDFstuff\lib-external\szip\bin\windows\szip-msvc++\lib\szlib.lib
SZIPLIB=


#===========================================================================
#   Do not make any change below this line unless you know what you do
#===========================================================================
PATH=$(PATH);$(VCPPDIR)\BIN
SRCDIR=$(HDFJAVADIR)

VALID_PATH_SET=YES
#-------------------------------------------------------
# Test if all path is valid

!IF EXISTS("$(VCPPDIR)")
!ELSE
!MESSAGE ERROR: Visual C++ directory $(VCPPDIR) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(JAVADIR)")
!ELSE
!MESSAGE ERROR: JDK directory $(JAVADIR) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(SRCDIR)")
!ELSE
!MESSAGE ERROR: C source directory $(SRCDIR) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(HDFLIBDIR)")
!ELSE
!MESSAGE ERROR: HDF library directory $(HDFLIBDIR) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(HDFINCDIR)")
!ELSE
!MESSAGE ERROR: HDF header directory $(HDFINCDIR) does not exist
VALID_PATH_SET=NO 
!ENDIF

#!IF EXISTS("$(JPEGLIB)")
#!ELSE
#!MESSAGE ERROR: JPEG library does not exist
#VALID_PATH_SET=NO 
#!ENDIF

!IF EXISTS("$(GZIPLIB)")
!ELSE
!MESSAGE ERROR: GZIP library does not exist
VALID_PATH_SET=NO 
!ENDIF

#!IF EXISTS("$(SZIPLIB)")
#!ELSE
#!MESSAGE ERROR: SZIP library does not exist
#VALID_PATH_SET=NO 
#!ENDIF

#-------------------------------------------------------


!IF "$(VALID_PATH_SET)" == "YES"

!IF "$(OS)" == "Windows_NT"
NULL=
!ELSE 
NULL=nul
!ENDIF 

INTDIR=.\jhdf5\Release
OUTDIR=$(HDFJAVADIR)\lib\win

INCLUDES =  \
	"$(JAVADIR)\include\jni.h" \
	"$(JAVADIR)\include\win32\jni_md.h" \
	"$(SRCDIR)\h5Constants.h" \
	"$(HDFINCDIR)\H5ACpublic.h" \
	"$(HDFINCDIR)\H5api_adpt.h" \
	"$(HDFINCDIR)\H5Apkg.h" \
	"$(HDFINCDIR)\H5Apublic.h" \
	"$(HDFINCDIR)\H5Bpkg.h" \
	"$(HDFINCDIR)\H5Bpublic.h" \
#	"$(HDFINCDIR)\H5config.h" \
	"$(HDFINCDIR)\H5Dpkg.h" \
	"$(HDFINCDIR)\H5Dpublic.h" \
	"$(HDFINCDIR)\H5Epublic.h" \
	"$(HDFINCDIR)\H5FDcore.h" \
	"$(HDFINCDIR)\H5FDfamily.h" \
#	"$(HDFINCDIR)\H5FDfphdf5.h" \
#	"$(HDFINCDIR)\H5FDgass.h" \
	"$(HDFINCDIR)\H5FDlog.h" \
	"$(HDFINCDIR)\H5FDmpio.h" \
	"$(HDFINCDIR)\H5FDmpiposix.h" \
	"$(HDFINCDIR)\H5FDmulti.h" \
	"$(HDFINCDIR)\H5FDpublic.h" \
	"$(HDFINCDIR)\H5FDsec2.h" \
#	"$(HDFINCDIR)\H5FDsrb.h" \
	"$(HDFINCDIR)\H5FDstdio.h" \
#	"$(HDFINCDIR)\H5FDstream.h" \
	"$(HDFINCDIR)\H5Fpkg.h" \
#	"$(HDFINCDIR)\H5FPpublic.h" \
	"$(HDFINCDIR)\H5Fpublic.h" \
	"$(HDFINCDIR)\H5Gpkg.h" \
	"$(HDFINCDIR)\H5Gpublic.h" \
	"$(HDFINCDIR)\H5HGpublic.h" \
	"$(HDFINCDIR)\H5HLpublic.h" \
	"$(HDFINCDIR)\H5Ipkg.h" \
	"$(HDFINCDIR)\H5Ipublic.h" \
	"$(HDFINCDIR)\H5MMpublic.h" \
	"$(HDFINCDIR)\H5Opkg.h" \
	"$(HDFINCDIR)\H5Opublic.h" \
	"$(HDFINCDIR)\H5Ppkg.h" \
	"$(HDFINCDIR)\H5Ppublic.h" \
	"$(HDFINCDIR)\H5pubconf.h" \
	"$(HDFINCDIR)\H5public.h" \
	"$(HDFINCDIR)\H5Rpublic.h" \
	"$(HDFINCDIR)\H5Spkg.h" \
	"$(HDFINCDIR)\H5Spublic.h" \
	"$(HDFINCDIR)\H5Tpkg.h" \
	"$(HDFINCDIR)\H5Tpublic.h" \
	"$(HDFINCDIR)\H5Zpkg.h" \
	"$(HDFINCDIR)\H5Zpublic.h" \
 	"$(HDFINCDIR)\hdf5.h"


ALL : "$(OUTDIR)\jhdf5.dll"

"$(INTDIR)" :
    if not exist "$(INTDIR)/$(NULL)" mkdir "$(INTDIR)"

"$(OUTDIR)" :
    if not exist "$(OUTDIR)/$(NULL)" mkdir "$(OUTDIR)"

CPP=cl.exe
CPP_PROJ=/nologo /W3 /EHsc /O2 /I "$(HDFINCDIR)" /I "$(JAVADIR)\include" /I "$(JAVADIR)\include\win32" /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /Fp"$(INTDIR)\jhdf5.pch" /Fo"$(INTDIR)\\" /Fd"$(INTDIR)\\" /FD /c 

.c{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.c{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

MTL=midl.exe
MTL_PROJ=/nologo /D "NDEBUG" /mktyplib203 /win32 
RSC=rc.exe
BSC32=bscmake.exe
BSC32_FLAGS=/nologo /o"$(INTDIR)\jhdf5.bsc" 
BSC32_SBRS= \
	
LINK64=link.exe
LINK64_FLAGS=$(HDFLIBDIR)\hdf5.lib $(SZIPLIB) $(GZIPLIB) /nologo /dll /nodefaultlib:msvcrt /incremental:no /pdb:"$(INTDIR)\jhdf5.pdb" /machine:x64 /out:"$(OUTDIR)\jhdf5.dll" /implib:"$(INTDIR)\jhdf5.lib" 
LINK64_OBJS= \
	"$(INTDIR)\exceptionImp.obj" \
	"$(INTDIR)\h5aImp.obj" \
	"$(INTDIR)\h5Constants.obj" \
	"$(INTDIR)\h5dImp.obj" \
	"$(INTDIR)\h5fImp.obj" \
	"$(INTDIR)\h5gImp.obj" \
	"$(INTDIR)\h5iImp.obj" \
	"$(INTDIR)\h5Imp.obj" \
	"$(INTDIR)\h5lImp.obj" \
	"$(INTDIR)\h5oImp.obj" \
	"$(INTDIR)\h5pImp.obj" \
	"$(INTDIR)\h5rImp.obj" \
	"$(INTDIR)\h5sImp.obj" \
	"$(INTDIR)\h5tImp.obj" \
	"$(INTDIR)\h5util.obj" \
	"$(INTDIR)\h5zImp.obj" \
	"$(INTDIR)\nativeData.obj"

"$(OUTDIR)\jhdf5.dll" : "$(OUTDIR)" $(DEF_FILE) $(LINK64_OBJS)
    $(LINK64) @<<
  $(LINK64_FLAGS) $(LINK64_OBJS)
<<


SOURCE=$(SRCDIR)\exceptionImp.c

"$(INTDIR)\exceptionImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5aImp.c

"$(INTDIR)\h5aImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5Constants.c

"$(INTDIR)\h5Constants.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5dImp.c

"$(INTDIR)\h5dImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5fImp.c

"$(INTDIR)\h5fImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5gImp.c

"$(INTDIR)\h5gImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5iImp.c

"$(INTDIR)\h5iImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5Imp.c

"$(INTDIR)\h5Imp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5lImp.c

"$(INTDIR)\h5lImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5oImp.c

"$(INTDIR)\h5oImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5pImp.c

"$(INTDIR)\h5pImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5rImp.c

"$(INTDIR)\h5rImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5sImp.c

"$(INTDIR)\h5sImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5tImp.c

"$(INTDIR)\h5tImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5util.c

"$(INTDIR)\h5util.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\h5zImp.c

"$(INTDIR)\h5zImp.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR)\nativeData.c

"$(INTDIR)\nativeData.obj" : $(SOURCE) $(INCLUDES) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


CLEAN :
	-@erase "$(INTDIR)\exceptionImp.obj"
	-@erase "$(INTDIR)\h5aImp.obj"
	-@erase "$(INTDIR)\h5Constants.obj"
	-@erase "$(INTDIR)\h5dImp.obj"
	-@erase "$(INTDIR)\h5fImp.obj"
	-@erase "$(INTDIR)\h5gImp.obj"
	-@erase "$(INTDIR)\h5iImp.obj"
	-@erase "$(INTDIR)\h5lImp.obj"
	-@erase "$(INTDIR)\h5Imp.obj"
	-@erase "$(INTDIR)\h5pImp.obj"
	-@erase "$(INTDIR)\h5rImp.obj"
	-@erase "$(INTDIR)\h5sImp.obj"
	-@erase "$(INTDIR)\h5tImp.obj"
	-@erase "$(INTDIR)\h5zImp.obj"
	-@erase "$(INTDIR)\h5util.obj"
	-@erase "$(INTDIR)\nativeData.obj"
	-@erase "$(INTDIR)\vc80.idb"
	-@erase "$(INTDIR)\jhdf5.exp"
	-@erase "$(INTDIR)\jhdf5.lib"
	-@erase "$(OUTDIR)\jhdf5.dll"

!ENDIF
