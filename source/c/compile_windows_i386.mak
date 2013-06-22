#============================================================================
#
#              Makefile to compile HDF Java Native C Source
#              Usage: nmake /f compile_windows_i386.mak
#
#============================================================================

# Visual C++ directory, for example
VCPPDIR=C:\Program Files\Microsoft Visual Studio 9.0\VC

# Directory where JDK is installed (We require JDK 1.6)
JAVADIR=C:\Program Files\Java\jdk1.6.0_37

# Common parent directory
HDFPARENTDIR=C:\JHDF5

# Directory of the HDF Java Products, for example
HDFJAVADIR=$(HDFPARENTDIR)\jhdf5_src\

# The directory where HDF5 has been compiled
HDFDIR=$(HDFPARENTDIR)\hdf5\hdf5-1.8.11

# The directory where HDF library is located
HDFBUILDDIR=$(HDFDIR)\build

# The directory where HDF library is located
HDFLIBDIR=$(HDFBUILDDIR)\bin\Release

# The directory where HDF header files are located
HDFINCDIR=$(HDFDIR)\src

# the JPEG library, for example
#JPEGLIB=E:\Work\MyHDFstuff\lib-external\jpeg\libjpeg.lib
JPEGLIB=

# the GZIP library, for example
#GZIPLIB=E:\Work\MyHDFstuff\lib-external\zlib\bin\windows\zlib114\lib\zlib.lib
GZIPLIB=$(HDFLIBDIR)\zlibstatic.lib

# SZIP library, for example
#SZIPLIB=E:\Work\MyHDFstuff\lib-external\szip\bin\windows\szip-msvc++\lib\szlib.lib
SZIPLIB=


#===========================================================================
#   Do not make any change below this line unless you know what you do
#===========================================================================
PATH=$(PATH);$(VCPPDIR)\BIN
SRCDIR1=$(HDFJAVADIR)\jhdf5
SRCDIR2=$(HDFJAVADIR)\hdf-java

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

!IF EXISTS("$(SRCDIR1)")
!ELSE
!MESSAGE ERROR: C source directory $(SRCDIR1) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(SRCDIR2)")
!ELSE
!MESSAGE ERROR: C source directory $(SRCDIR2) does not exist
VALID_PATH_SET=NO 
!ENDIF

!IF EXISTS("$(HDFBUILDDIR)")
!ELSE
!MESSAGE ERROR: HDF build directory $(HDFBUILDDIR) does not exist
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

ALL : "$(OUTDIR)\jhdf5.dll"

"$(INTDIR)" :
    if not exist "$(INTDIR)/$(NULL)" mkdir "$(INTDIR)"

"$(OUTDIR)" :
    if not exist "$(OUTDIR)/$(NULL)" mkdir "$(OUTDIR)"

CPP=cl.exe
CPP_PROJ=/nologo /W3 /EHsc /O2 /I "$(HDFINCDIR)" /I "$(HDFBUILDDIR)" /I "$(JAVADIR)\include" /I "$(JAVADIR)\include\win32" /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /Fp"$(INTDIR)\jhdf5.pch" /Fo"$(INTDIR)\\" /Fd"$(INTDIR)\\" /FD /c 

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
	
LINK32=link.exe
LINK32_FLAGS=$(HDFLIBDIR)\libhdf5.lib $(SZIPLIB) $(GZIPLIB) /nologo /dll /nodefaultlib:msvcrt /incremental:no /pdb:"$(INTDIR)\jhdf5.pdb" /machine:I386 /out:"$(OUTDIR)\jhdf5.dll" /implib:"$(INTDIR)\jhdf5.lib" 
LINK32_OBJS= \
	"$(INTDIR)\exceptionImpJHDF5.obj" \
	"$(INTDIR)\h5aImpJHDF5.obj" \
	"$(INTDIR)\h5ConstantsJHDF5.obj" \
	"$(INTDIR)\h5dImpJHDF5.obj" \
	"$(INTDIR)\h5fImpJHDF5.obj" \
	"$(INTDIR)\h5gImpJHDF5.obj" \
	"$(INTDIR)\h5iImpJHDF5.obj" \
	"$(INTDIR)\h5ImpJHDF5.obj" \
	"$(INTDIR)\h5lImpJHDF5.obj" \
	"$(INTDIR)\h5oImpJHDF5.obj" \
	"$(INTDIR)\h5pImpJHDF5.obj" \
	"$(INTDIR)\h5rImpJHDF5.obj" \
	"$(INTDIR)\h5sImpJHDF5.obj" \
	"$(INTDIR)\h5tImpJHDF5.obj" \
	"$(INTDIR)\h5utilJHDF5.obj" \
	"$(INTDIR)\h5zImpJHDF5.obj" \
	"$(INTDIR)\h5aImp.obj" \
	"$(INTDIR)\h5Constants.obj" \
	"$(INTDIR)\h5dImp.obj" \
	"$(INTDIR)\h5eImp.obj" \
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

"$(OUTDIR)\jhdf5.dll" : "$(OUTDIR)" $(DEF_FILE) $(LINK32_OBJS)
    $(LINK32) @<<
  $(LINK32_FLAGS) $(LINK32_OBJS)
<<


SOURCE=$(SRCDIR1)\exceptionImpJHDF5.c

"$(INTDIR)\exceptionImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5aImpJHDF5.c

"$(INTDIR)\h5aImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5ConstantsJHDF5.c

"$(INTDIR)\h5ConstantsJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5dImpJHDF5.c

"$(INTDIR)\h5dImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5fImpJHDF5.c

"$(INTDIR)\h5fImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5gImpJHDF5.c

"$(INTDIR)\h5gImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5iImpJHDF5.c

"$(INTDIR)\h5iImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5ImpJHDF5.c

"$(INTDIR)\h5ImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5lImpJHDF5.c

"$(INTDIR)\h5lImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5oImpJHDF5.c

"$(INTDIR)\h5oImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5pImpJHDF5.c

"$(INTDIR)\h5pImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5rImpJHDF5.c

"$(INTDIR)\h5rImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5sImpJHDF5.c

"$(INTDIR)\h5sImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5tImpJHDF5.c

"$(INTDIR)\h5tImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5utilJHDF5.c

"$(INTDIR)\h5utilJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR1)\h5zImpJHDF5.c

"$(INTDIR)\h5zImpJHDF5.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)



SOURCE=$(SRCDIR2)\h5aImp.c

"$(INTDIR)\h5aImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5Constants.c

"$(INTDIR)\h5Constants.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5dImp.c

"$(INTDIR)\h5dImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5eImp.c

"$(INTDIR)\h5eImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5fImp.c

"$(INTDIR)\h5fImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5gImp.c

"$(INTDIR)\h5gImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5iImp.c

"$(INTDIR)\h5iImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5Imp.c

"$(INTDIR)\h5Imp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5lImp.c

"$(INTDIR)\h5lImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5oImp.c

"$(INTDIR)\h5oImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5pImp.c

"$(INTDIR)\h5pImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5rImp.c

"$(INTDIR)\h5rImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5sImp.c

"$(INTDIR)\h5sImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5tImp.c

"$(INTDIR)\h5tImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5util.c

"$(INTDIR)\h5util.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\h5zImp.c

"$(INTDIR)\h5zImp.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=$(SRCDIR2)\nativeData.c

"$(INTDIR)\nativeData.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)



CLEAN :
	-@erase "$(INTDIR)\exceptionImpJHDF5.obj"
	-@erase "$(INTDIR)\h5aImpJHDF5.obj"
	-@erase "$(INTDIR)\h5ConstantsJHDF5.obj"
	-@erase "$(INTDIR)\h5dImpJHDF5.obj"
	-@erase "$(INTDIR)\h5fImpJHDF5.obj"
	-@erase "$(INTDIR)\h5gImpJHDF5.obj"
	-@erase "$(INTDIR)\h5iImpJHDF5.obj"
	-@erase "$(INTDIR)\h5lImpJHDF5.obj"
	-@erase "$(INTDIR)\h5ImpJHDF5.obj"
	-@erase "$(INTDIR)\h5pImpJHDF5.obj"
	-@erase "$(INTDIR)\h5rImpJHDF5.obj"
	-@erase "$(INTDIR)\h5sImpJHDF5.obj"
	-@erase "$(INTDIR)\h5tImpJHDF5.obj"
	-@erase "$(INTDIR)\h5oImpJHDF5.obj"
	-@erase "$(INTDIR)\h5zImpJHDF5.obj"
	-@erase "$(INTDIR)\h5utilJHDF5.obj"
	-@erase "$(INTDIR)\h5aImp.obj"
	-@erase "$(INTDIR)\h5Constants.obj"
	-@erase "$(INTDIR)\h5dImp.obj"
	-@erase "$(INTDIR)\h5eImp.obj"
	-@erase "$(INTDIR)\h5fImp.obj"
	-@erase "$(INTDIR)\h5gImp.obj"
	-@erase "$(INTDIR)\h5iImp.obj"
	-@erase "$(INTDIR)\h5lImp.obj"
	-@erase "$(INTDIR)\h5Imp.obj"
	-@erase "$(INTDIR)\h5pImp.obj"
	-@erase "$(INTDIR)\h5rImp.obj"
	-@erase "$(INTDIR)\h5sImp.obj"
	-@erase "$(INTDIR)\h5tImp.obj"
	-@erase "$(INTDIR)\h5oImp.obj"
	-@erase "$(INTDIR)\h5zImp.obj"
	-@erase "$(INTDIR)\h5util.obj"
	-@erase "$(INTDIR)\nativeData.obj"
	-@erase "$(INTDIR)\vc90.idb"
	-@erase "$(INTDIR)\jhdf5.exp"
	-@erase "$(INTDIR)\jhdf5.lib"
	-@erase "$(OUTDIR)\jhdf5.dll"

!ENDIF
