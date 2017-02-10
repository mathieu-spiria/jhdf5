/****************************************************************************
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                          *
 * This file is part of HDF Java Products. The full HDF Java copyright       *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the file, COPYING.  COPYING can be found at the root of   *
 * the source code distribution tree. You can also access it online  at      *
 * http://www.hdfgroup.org/products/licenses.html.  If you do not have       *
 * access to the file, you may request a copy from help@hdfgroup.org.        *
 ****************************************************************************/

/*
 *  This is a utility program used by the HDF Java-C wrapper layer to
 *  generate exceptions.  This may be called from any part of the
 *  Java-C interface.
 *
 */
#ifdef __cplusplus
extern "C" {
#endif

#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include "jni.h"
#include "../hdf-java/h5jni.h"

/*
#include "H5Eprivate.h"
*/
/*  These types are copied from H5Eprivate.h
 *  They should be moved to a public include file, and deleted from
 *  here.
 */
#define H5E_NSLOTS      32      /*number of slots in an error stack */
/*
* The list of error messages in the system is kept as an array of
* error_code/message pairs, one for major error numbers and another for
* minor error numbers.
*/
typedef struct H5E_major_mesg_t {
    H5E_major_t error_code;
    const char  *str;
} H5E_major_mesg_t;

typedef struct H5E_minor_mesg_t {
    H5E_minor_t error_code;
    const char  *str;
} H5E_minor_mesg_t;

/* major and minor error numbers */
typedef struct H5E_num_t {
    int maj_num;
    int min_num;
} H5E_num_t;

int getMajorErrorNumber();
int getMinorErrorNumber();
int getErrorNumbers(hid_t stk_id, H5E_num_t*);

/* get the major and minor error numbers on the top of the error stack */
static
herr_t walk_error_callback(unsigned n, const H5E_error2_t *err_desc, void *_err_nums)
{
    H5E_num_t *err_nums = (H5E_num_t *)_err_nums;

    if (err_desc) {
        err_nums->maj_num = err_desc->maj_num;
        err_nums->min_num = err_desc->min_num;
    }

    return 0;
}


char *defineHDF5LibraryException(int maj_num);


/*
 * Class:     ch_systemsx_cisd_hdf5_hdf5lib_H5_H5error_1off
 * Method:    H5error_off
 * Signature: ()I
 *
 */
JNIEXPORT jint JNICALL Java_ch_systemsx_cisd_hdf5_hdf5lib_H5_H5error_1off
  (JNIEnv *env, jclass clss )
{
    H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    return 0;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5_H5error_1off
 * Method:    H5error_off
 * Signature: ()I
 *
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5error_1off
  (JNIEnv *env, jclass clss )
{
    H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    return 0;
}

/*
 * Class:     ncsa_hdf_hdf5lib_exceptions_HDFLibraryException
 * Method:    printStackTrace0
 * Signature: (Ljava/lang/Object;)V
 *
 *  Call the HDF-5 library to print the HDF-5 error stack to 'file_name'.
 */
JNIEXPORT void JNICALL Java_ncsa_hdf_hdf5lib_exceptions_HDF5LibraryException_printStackTrace0
  (JNIEnv *env, jobject obj, jstring file_name)
{
    FILE *stream;
    char *file;

    if (file_name == NULL) {
        H5Eprint2(H5E_DEFAULT, stderr);
    }
    else {
        file = (char *)ENVPTR->GetStringUTFChars(ENVPAR file_name,0);
        stream = fopen(file, "a+");
        H5Eprint2(H5E_DEFAULT, stream);
        ENVPTR->ReleaseStringUTFChars(ENVPAR file_name, file);
        if (stream) fclose(stream);
    }
}

/*
 * Class:     ncsa_hdf_hdf5lib_exceptions_HDFLibraryException
 * Method:    getMajorErrorNumber
 * Signature: ()I
 *
 *  Extract the HDF-5 major error number from the HDF-5 error stack.
 *
 *  Note:  This relies on undocumented, 'private' code in the HDF-5
 *  library.  Later releases will have a public interface for this
 *  purpose.
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_exceptions_HDF5LibraryException_getMajorErrorNumber
  (JNIEnv *env, jobject obj)
{
    H5E_num_t err_nums;

    H5Ewalk2(H5E_DEFAULT, H5E_WALK_DOWNWARD, walk_error_callback, &err_nums);

    return (int) err_nums.maj_num;
}

int getMajorErrorNumber()
{
    H5E_num_t err_nums;
    err_nums.maj_num = 0;
    err_nums.min_num = 0;

    H5Ewalk2(H5E_DEFAULT, H5E_WALK_DOWNWARD, walk_error_callback, &err_nums);

    return (int) err_nums.maj_num;
}

/*
 * Class:     ncsa_hdf_hdf5lib_exceptions_HDFLibraryException
 * Method:    getMinorErrorNumber
 * Signature: ()I
 *
 *  Extract the HDF-5 minor error number from the HDF-5 error stack.
 *
 *  Note:  This relies on undocumented, 'private' code in the HDF-5
 *  library.  Later releases will have a public interface for this
 *  purpose.
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_exceptions_HDF5LibraryException_getMinorErrorNumber
  (JNIEnv *env, jobject obj)
{
    return (jint) getMinorErrorNumber();
}

int getMinorErrorNumber()
{
    H5E_num_t err_nums;
    err_nums.maj_num = 0;
    err_nums.min_num = 0;

    H5Ewalk2(H5E_DEFAULT, H5E_WALK_DOWNWARD, walk_error_callback, &err_nums);

    return (int) err_nums.min_num;
}

int getErrorNumbers(hid_t stk_id, H5E_num_t *err_nums)
{
    err_nums->maj_num = 0;
    err_nums->min_num = 0;
    return H5Ewalk2(stk_id, H5E_WALK_DOWNWARD, walk_error_callback, err_nums);
}

/*
 *  Routine to raise particular Java exceptions from C
 */

/*
 *  Create and throw an 'outOfMemoryException'
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5outOfMemory( JNIEnv *env, char *functName)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/OutOfMemoryError");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR functName);
    args[0] = (char *)str;
    args[1] = 0;

    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable ) ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  OutOfMemoryError: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}


/*
 *  A fatal error in a JNI call
 *  Create and throw an 'InternalError'
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5JNIFatalError( JNIEnv *env, char *functName)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/InternalError");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR functName);
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR  (jthrowable) ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  JNIFatal: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*
 *  A NULL argument in an HDF5 call
 *  Create and throw an 'NullPointerException'
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5nullArgument( JNIEnv *env, char *functName)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/NullPointerException");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR functName);
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable) ex );

    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  NullPointer: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*
 *  A bad argument in an HDF5 call
 *  Create and throw an 'IllegalArgumentException'
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5badArgument( JNIEnv *env, char *functName)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/IllegalArgumentException");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR functName);
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable) ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  BadArgument: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*
 *  Some feature Not implemented yet
 *  Create and throw an 'UnsupportedOperationException'
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5unimplemented( JNIEnv *env, char *functName)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/UnsupportedOperationException");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR functName);
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable) ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  Unsupported: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*
 *  h5libraryError()   determines the HDF-5 major error code
 *  and creates and throws the appropriate sub-class of
 *  HDF5LibraryException().  This routine should be called
 *  whenever a call to the HDF-5 library fails, i.e., when
 *  the return is -1.
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5libraryError( JNIEnv *env )
{
    jmethodID jm;
    jclass jc;
    jvalue args[4];
    char *exception;
    jobject ex;
    jstring min_msg_str = NULL, maj_msg_str = NULL;
    char *min_msg, *maj_msg;
    int rval, min_num, maj_num;
    ssize_t min_msg_size = 0, maj_msg_size = 0;
    H5E_type_t min_error_msg_type, maj_error_msg_type;
    hid_t stk_id = -1;
    H5E_num_t exceptionNumbers;

    /* Save current stack contents for future use */
    stk_id = H5Eget_current_stack(); /* This will clear current stack  */
    getErrorNumbers(stk_id, &exceptionNumbers);
    maj_num = exceptionNumbers.maj_num;
    min_num = exceptionNumbers.min_num;

    exception = (char *)defineHDF5LibraryException(maj_num);
    jc = ENVPTR->FindClass(ENVPAR exception);
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(ILjava/lang/String;ILjava/lang/String;)V");
    if (jm == NULL) {
        fprintf(stderr, "FATAL ERROR:  h5libraryError: Cannot find constructor\n");
        return JNI_FALSE;
    }

    /* get the length of the name (maj_error) */
    maj_msg_size = H5Eget_msg(maj_num, NULL, NULL, 0);
    if(maj_msg_size>0) {
        maj_msg_size++; /* add extra space for the null terminator */
        maj_msg = (char*)malloc(sizeof(char)*maj_msg_size);
        if(maj_msg) {
            maj_msg_size = H5Eget_msg((hid_t)maj_num, &maj_error_msg_type, (char *)maj_msg, (size_t)maj_msg_size);
            maj_msg_str = ENVPTR->NewStringUTF(ENVPAR maj_msg);
            free(maj_msg);
        }
    }
    else
        maj_msg_str = NULL;

    /* get the length of the name (min_error) */
    min_msg_size = H5Eget_msg(min_num, NULL, NULL, 0);
    if(min_msg_size>0) {
        min_msg_size++; /* add extra space for the null terminator */
        min_msg = (char*)malloc(sizeof(char)*min_msg_size);
        if(min_msg) {
            min_msg_size = H5Eget_msg((hid_t)min_num, &min_error_msg_type, (char *)min_msg, (size_t)min_msg_size);
            min_msg_str = ENVPTR->NewStringUTF(ENVPAR min_msg);
            free(min_msg);
        }
    }
    else
        min_msg_str = NULL;
    H5Eset_current_stack(stk_id);

    if (maj_msg_str == NULL || min_msg_str == NULL)
    {
        fprintf(stderr, "FATAL ERROR: h5libraryError: Out of Memory\n");
        return JNI_FALSE;
    }

    args[0].i = maj_num;
    args[1].l = maj_msg_str;
    args[2].i = min_num;
    args[3].l = min_msg_str;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable) ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  h5libraryError: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}


/*
 *  A constant that is not defined in J2C throws an 'IllegalArgumentException'.
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5illegalConstantError(JNIEnv *env)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR "java/lang/IllegalArgumentException");
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR "Illegal java constant");
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable)ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  Unsupported: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*  raiseException().  This routine is called to generate
 *  an arbitrary Java exception with a particular message.
 *
 *  Note:  This routine never returns from the 'throw',
 *  and the Java native method immediately raises the
 *  exception.
 */
jboolean h5raiseException( JNIEnv *env, char *exception, char *message)
{
    jmethodID jm;
    jclass jc;
    char * args[2];
    jobject ex;
    jstring str;
    int rval;

    jc = ENVPTR->FindClass(ENVPAR exception);
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(Ljava/lang/String;)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }

    str = ENVPTR->NewStringUTF(ENVPAR message);
    args[0] = (char *)str;
    args[1] = 0;
    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );
    rval = ENVPTR->Throw(ENVPAR (jthrowable)ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  raiseException: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

/*
jboolean buildException( JNIEnv *env, char *exception, jint HDFerr)
{
    jmethodID jm;
    jclass jc;
    int args[2];
    jobject ex;
    int rval;


    jc = ENVPTR->FindClass(ENVPAR exception);
    if (jc == NULL) {
        return JNI_FALSE;
    }
    jm = ENVPTR->GetMethodID(ENVPAR jc, "<init>", "(I)V");
    if (jm == NULL) {
        return JNI_FALSE;
    }
    args[0] = HDFerr;
    args[1] = 0;

    ex = ENVPTR->NewObjectA (ENVPAR jc, jm, (jvalue *)args );

    rval = ENVPTR->Throw(ENVPAR  ex );
    if (rval < 0) {
        fprintf(stderr, "FATAL ERROR:  raiseException: Throw failed\n");
        return JNI_FALSE;
    }

    return JNI_TRUE;
}
*/

/*
 *  defineHDF5LibraryException()  returns the name of the sub-class
 *  which goes with an HDF-5 error code.
 */
char *defineHDF5LibraryException(int maj_num)
{
    H5E_major_t err_num = (H5E_major_t) maj_num;

    if (H5E_ARGS == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5FunctionArgumentException";
    else if (H5E_RESOURCE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5ResourceUnavailableException";
    else if (H5E_INTERNAL == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5InternalErrorException";
    else if (H5E_FILE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5FileInterfaceException";
    else if (H5E_IO == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5LowLevelIOException";
    else if (H5E_FUNC == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5FunctionEntryExitException";
    else if (H5E_ATOM == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5AtomException";
    else if (H5E_CACHE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5MetaDataCacheException";
    else if (H5E_BTREE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5BtreeException";
    else if (H5E_SYM == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5SymbolTableException";
    else if (H5E_HEAP == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5HeapException";
    else if (H5E_OHDR == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5ObjectHeaderException";
    else if (H5E_DATATYPE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5DatatypeInterfaceException";
    else if (H5E_DATASPACE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5DataspaceInterfaceException";
    else if (H5E_DATASET == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5DatasetInterfaceException";
    else if (H5E_STORAGE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5DataStorageException";
    else if (H5E_PLIST == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5PropertyListInterfaceException";
    else if (H5E_ATTR == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5AttributeException";
    else if (H5E_PLINE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5DataFiltersException";
    else if (H5E_EFL == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5ExternalFileListException";
    else if (H5E_REFERENCE == err_num)
        return "ncsa/hdf/hdf5lib/exceptions/HDF5ReferenceException";
    return "ncsa/hdf/hdf5lib/exceptions/HDF5LibraryException";

}

#ifdef __cplusplus
}
#endif
