/****************************************************************************
 * NCSA HDF                                                                 *
 * National Comptational Science Alliance                                   *
 * University of Illinois at Urbana-Champaign                               *
 * 605 E. Springfield, Champaign IL 61820                                   *
 *                                                                          *
 * For conditions of distribution and use, see the accompanying             *
 * hdf-java/COPYING file.                                                   *
 *                                                                          *
 ****************************************************************************/

/*
 *  This code is the C-interface called by Java programs to access the
 *  Group Object API Functions of the HDF5 library.
 *
 *  Each routine wraps a single HDF entry point, generally with the
 *  analogous arguments and return codes.
 *
 *  For details of the HDF libraries, see the HDF Documentation at:
 *   http://hdf.ncsa.uiuc.edu/HDF5/doc/
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "hdf5.h"
/* missing definitions from hdf5.h */
#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE (!FALSE)
#endif

/* delete TRUE and FALSE when fixed in HDF5 */

#include <jni.h>
#include <stdlib.h>

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    HOGopen
 * Signature: (ILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Oopen
  (JNIEnv *env, jclass clss, jint loc_id, jstring name, jint access_plist_id)
{
    herr_t status;
    char* gName;
    jboolean isCopy;

    if (name == NULL) {
        h5nullArgument( env, "H5Oopen:  name is NULL");
        return -1;
    }

#ifdef __cplusplus
    gName = (char *)env->GetStringUTFChars(name,&isCopy);
#else
    gName = (char *)(*env)->GetStringUTFChars(env,name,&isCopy);
#endif

    if (gName == NULL) {
        h5JNIFatalError( env, "H5Oopen:  file name not pinned");
        return -1;
    }

    status = H5Oopen((hid_t)loc_id, gName, (hid_t) access_plist_id);

#ifdef __cplusplus
    env->ReleaseStringUTFChars(name,gName);
#else
    (*env)->ReleaseStringUTFChars(env,name,gName);
#endif
    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Oclose
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Oclose
  (JNIEnv *env, jclass clss, jint group_id)
{
    herr_t retVal = 0;

	if (group_id > 0)
        retVal =  H5Oclose((hid_t)group_id) ;

    if (retVal < 0) {
        h5libraryError(env);
    }

    return (jint)retVal;
}
