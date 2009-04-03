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

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Oget_info_by_name
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Oget_1info_1by_1name
  (JNIEnv *env, jclass clss, jint loc_id, jstring object_name,
    jlongArray info, jboolean exception_when_non_existent)
{
    jint type;
    herr_t status;
    jlong *infoP, *infoPP;
    jint info_len;
    int minor_err_num;
    char *oName;
    jboolean isCopy;
    H5O_info_t obj_info;

    if (object_name == NULL) {
        h5nullArgument( env, "H5Oget_info_by_name:  object_name is NULL");
        return -1;
    }
    if (info != NULL)
    {
#ifdef __cplusplus
	    info_len = env->GetArrayLength(info);
#else
	    info_len = (*env)->GetArrayLength(env,info);
#endif
			if (info_len != 5)
			{
			  h5badArgument( env, "H5Oget_info_by_name:  info is not an array of length 5");
			}        
    }

#ifdef __cplusplus
    oName = (char *)env->GetStringUTFChars(object_name,&isCopy);
#else
    oName = (char *)(*env)->GetStringUTFChars(env,object_name,&isCopy);
#endif
    if (oName == NULL) {
        h5JNIFatalError( env, "H5Oget_info_by_name:  object_name not pinned");
        return -1;
    }

  	status = H5Oget_info_by_name(loc_id, oName, &obj_info, H5P_DEFAULT); 
    (*env)->ReleaseStringUTFChars(env,object_name,oName);
    if (status < 0)
    {
      if (exception_when_non_existent == JNI_FALSE)
      {
          minor_err_num = getMinorErrorNumber();
          /*
           * Note: H5E_CANTINSERT is thrown by the dense group lookup, see H5Gdense:534. That is
           * probably a wrong error code, but we have to deal with it here anyway.
           */
          if (minor_err_num  == H5E_NOTFOUND || minor_err_num == H5E_CANTINSERT)
          {
              return -1;
          }
      }
      h5libraryError(env);
      return -1;
    } else {
        type = obj_info.type;
        if (info != NULL)
        {
#ifdef __cplusplus
    			infoP = env->GetPrimitiveArrayCritical(info,&isCopy);
#else
    			infoP = (*env)->GetPrimitiveArrayCritical(env,info,&isCopy);
#endif
    			if (infoP == NULL) {
        		h5JNIFatalError( env, "H5Oget_info_by_name:  info not pinned");
		        return -1;
			    }
			    infoPP = infoP;
			    *infoPP++ = obj_info.fileno;
			    *infoPP++ = obj_info.addr;
			    *infoPP++ = obj_info.rc;
			    *infoPP++ = obj_info.ctime;
			    *infoPP++ = obj_info.num_attrs;
#ifdef __cplusplus
          env->ReleasePrimitiveArrayCritical(info,infoP,JNI_COMMIT);
#else
          (*env)->ReleasePrimitiveArrayCritical(env,info,infoP,JNI_COMMIT);
#endif
			    
			  }
    }

    return (jint) type;

}

