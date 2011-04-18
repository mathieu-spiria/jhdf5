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
 *  Reference API Functions of the HDF5 library.
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
#include <jni.h>
#include <stdlib.h>

extern jboolean h5JNIFatalError( JNIEnv *env, char *functName);
extern jboolean h5nullArgument( JNIEnv *env, char *functName);
extern jboolean h5badArgument( JNIEnv *env, char *functName);
extern jboolean h5libraryError( JNIEnv *env );

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Rcreate
 * Signature: ([BILjava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rcreate___3BILjava_lang_String_2II
  (JNIEnv *env, jclass clss,
  jbyteArray ref, jint loc_id, jstring name, jint ref_type, jint space_id)
{
    char* rName;
    herr_t status;
    jbyte *refP;

    if (ref == NULL) {
        h5nullArgument( env, "H5Rcreate:  ref is NULL");
        return -1;
    }
    if (name == NULL) {
        h5nullArgument( env, "H5Rcreate:  name is NULL");
        return -1;
    }
    if (ref_type == H5R_OBJECT) {
#ifdef __cplusplus
        if (env->GetArrayLength(ref) < 8) {
            h5badArgument( env, "H5Rcreate:  ref input array < 8");
            return -1;
        }
#else
        if ((*env)->GetArrayLength(env, ref) < 8) {
            h5badArgument( env, "H5Rcreate:  ref input array < 8");
            return -1;
        }
#endif
    } else if (ref_type == H5R_DATASET_REGION) {
#ifdef __cplusplus
        if (env->GetArrayLength( ref) < 12) {
            h5badArgument( env, "H5Rcreate:  region ref input array < 12");
            return -1;
        }
#else
        if ((*env)->GetArrayLength(env, ref) < 12) {
            h5badArgument( env, "H5Rcreate:  region ref input array < 12");
            return -1;
        }
#endif
    } else {
        h5badArgument( env, "H5Rcreate:  ref_type unknown type ");
        return -1;
    }

#ifdef __cplusplus
    refP = (jbyte *)env->GetByteArrayElements(ref,NULL);
#else
    refP = (jbyte *)(*env)->GetByteArrayElements(env,ref,NULL);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rcreate:  ref not pinned");
        return -1;
    }
#ifdef __cplusplus
    rName = (char *)env->GetStringUTFChars(name,NULL);
#else
    rName = (char *)(*env)->GetStringUTFChars(env,name,NULL);
#endif
    if (rName == NULL) {
#ifdef __cplusplus
        env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
        (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif
        h5JNIFatalError(env,  "H5Rcreate:  name not pinned");
        return -1;
    }

    status = H5Rcreate(refP, loc_id, rName, (H5R_type_t)ref_type, space_id);
#ifdef __cplusplus
    env->ReleaseStringUTFChars(name,rName);
#else
    (*env)->ReleaseStringUTFChars(env,name,rName);
#endif

    if (status < 0) {
#ifdef __cplusplus
        env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
        (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif
        h5libraryError(env);
        return -1;
    }

#ifdef __cplusplus
    env->ReleaseByteArrayElements(ref,refP,0);
#else
    (*env)->ReleaseByteArrayElements(env,ref,refP,0);
#endif

    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Rcreate
 * Signature: (ILjava/lang/String;)[J
 */
JNIEXPORT jlongArray JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rcreate__I_3Ljava_lang_String_2
  (JNIEnv *env, jclass clss, jint loc_id, jobjectArray names)
{
    char* rName;
    herr_t status;
    jint arrayLen;
    jlongArray array;
    jlong* arrayP;
    jlong* arrayPR;
    jstring name;
    int i;

    if (names == NULL) {
        h5nullArgument( env, "H5Rcreate:  names is NULL");
        return NULL;
    }

#ifdef __cplusplus
    arrayLen = env->GetArrayLength(names);
    array = env->NewLongArray(arrayLen);
    if (array == NULL) {
        return NULL;
    }
    arrayP = (long *)env->GetLongArrayElements(array,NULL);
#else
    arrayLen = (*env)->GetArrayLength(env,names);
    array = (*env)->NewLongArray(env,arrayLen);
    if (array == NULL) {
        return NULL;
    }
    arrayP = (jlong *)(*env)->GetLongArrayElements(env,array,NULL);
#endif
    if (arrayP == NULL) {
        h5JNIFatalError(env,  "H5Rcreate:  array not pinned");
        return NULL;
    }
    
    for (i = 0,arrayPR=arrayP; i < arrayLen; ++i,++arrayPR) { 
    
#ifdef __cplusplus
        name = env->GetObjectArrayElement(names,i);
#else
        name = (*env)->GetObjectArrayElement(env,names,i);
#endif
        if (name == NULL) {
#ifdef __cplusplus
            env->ReleaseLongArrayElements(array,arrayP,JNI_ABORT);
#else
            (*env)->ReleaseLongArrayElements(env,array,arrayP,JNI_ABORT);
#endif
            return NULL;
        }
#ifdef __cplusplus
        rName = (char *)env->GetStringUTFChars(name,NULL);
#else
        rName = (char *)(*env)->GetStringUTFChars(env,name,NULL);
#endif
        if (rName == NULL) {
#ifdef __cplusplus
            env->ReleaseLongArrayElements(array,arrayP,JNI_ABORT);
#else
            (*env)->ReleaseLongArrayElements(env,array,arrayP,JNI_ABORT);
#endif
            h5JNIFatalError(env,  "H5Rcreate:  name not pinned");
            return NULL;
        }

        status = H5Rcreate(arrayPR, loc_id, rName, H5R_OBJECT, -1);
#ifdef __cplusplus
        env->ReleaseStringUTFChars(name,rName);
#else
        (*env)->ReleaseStringUTFChars(env,name,rName);
#endif
        if (status < 0) {
#ifdef __cplusplus
            env->ReleaseLongArrayElements(array,arrayP,0);
#else
            (*env)->ReleaseLongArrayElements(env,array,arrayP,0);
#endif
            h5libraryError(env);
            return NULL;
        }
    
    } /* for (i=0...)*/

#ifdef __cplusplus
    env->ReleaseLongArrayElements(array,arrayP,0);
#else
    (*env)->ReleaseLongArrayElements(env,array,arrayP,0);
#endif

    return array;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Rdereference
 * Signature: (II[B)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rdereference
  (JNIEnv *env, jclass clss, jint dataset, jint ref_type,
  jbyteArray ref )
{
    jboolean isCopy;
    jbyte *refP;
    herr_t status;

    if (ref == NULL) {
        h5nullArgument( env, "H5Rdereference:  ref is NULL");
        return -1;
    }
#ifdef __cplusplus
    if ((ref_type == H5R_OBJECT) && env->GetArrayLength(ref) < 8) {
        h5badArgument( env, "H5Rdereference:  obj ref input array < 8");
    } else if ((ref_type == H5R_DATASET_REGION)
        && env->GetArrayLength(ref) < 12) {
        h5badArgument( env, "H5Rdereference:  region ref input array < 12");
    }
    refP = (jbyte *)env->GetByteArrayElements(ref,&isCopy);
#else
    if ((ref_type == H5R_OBJECT) && (*env)->GetArrayLength(env, ref) < 8) {
        h5badArgument( env, "H5Rdereference:  obj ref input array < 8");
    } else if ((ref_type == H5R_DATASET_REGION)
        && (*env)->GetArrayLength(env, ref) < 12) {
        h5badArgument( env, "H5Rdereference:  region ref input array < 12");
    }
    refP = (jbyte *)(*env)->GetByteArrayElements(env,ref,&isCopy);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rderefernce:  ref not pinned");
        return -1;
    }

    status = H5Rdereference((hid_t)dataset, (H5R_type_t)ref_type, refP);

#ifdef __cplusplus
    env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
    (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Rget_region
 * Signature: (II[B)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rget_1region
  (JNIEnv *env, jclass clss, jint dataset, jint ref_type,
  jbyteArray ref )
{
    hid_t status;
    jboolean isCopy;
    jbyte *refP;

    if (ref_type != H5R_DATASET_REGION)  {
        h5badArgument( env, "H5Rget_region:  bad ref_type ");
        return -1;
    }

    if (ref == NULL) {
        h5nullArgument( env, "H5Rget_region:  ref is NULL");
        return -1;
    }
#ifdef __cplusplus
    if ( env->GetArrayLength(ref) < 12) {
        h5badArgument( env, "H5Rget_region:  region ref input array < 12");
    }
    refP = (jbyte *)env->GetByteArrayElements(ref,&isCopy);
#else
    if ( (*env)->GetArrayLength(env, ref) < 12) {
        h5badArgument( env, "H5Rget_region:  region ref input array < 12");
    }
    refP = (jbyte *)(*env)->GetByteArrayElements(env,ref,&isCopy);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rget_region:  ref not pinned");
        return -1;
    }

    status = H5Rget_region((hid_t)dataset, (H5R_type_t)ref_type, refP);

#ifdef __cplusplus
    env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
    (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5G_obj_t H5Rget_obj_type(hid_t id, H5R_type_t ref_type, void *_ref)
 * Signature: (I[B)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rget_1obj_1type
  (JNIEnv *env, jclass clss, jint loc_id, jint ref_type, jbyteArray ref)
{

    herr_t status;
    H5O_type_t obj_type;
    jboolean isCopy;
    jbyte *refP;


    if (ref == NULL) {
        h5nullArgument( env, "H5Rget_object_type:  ref is NULL");
        return -1;
    }

#ifdef __cplusplus
    refP = (jbyte *)env->GetByteArrayElements(ref,&isCopy);
#else
    refP = (jbyte *)(*env)->GetByteArrayElements(env,ref,&isCopy);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rget_object_type:  ref not pinned");
        return -1;
    }

    status = H5Rget_obj_type((hid_t)loc_id, (H5R_type_t)ref_type, refP, &obj_type);

#ifdef __cplusplus
    env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
    (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)obj_type;
}

#ifdef __cplusplus
}
#endif

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    String H5Rget_name(hid_t id, H5R_type_t ref_type, void *_ref)
 * Signature: (I[B)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rget_1name__II_3B
  (JNIEnv *env, jclass clss, jint loc_id, jint ref_type, jbyteArray ref)
{
		ssize_t size;
    H5O_type_t obj_type;
    jbyte *refP;
    char *rName;
    int rname_buf_size = 128;
    jstring str;

    if (ref == NULL) {
        h5nullArgument( env, "H5Rget_name:  ref is NULL");
        return NULL;
    }

#ifdef __cplusplus
    refP = (jbyte *)env->GetByteArrayElements(ref, NULL);
#else
    refP = (jbyte *)(*env)->GetByteArrayElements(env,ref, NULL);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rget_name:  ref not pinned");
        return NULL;
    }

    rName = (char*) malloc(sizeof(char) * rname_buf_size);
    if (rName == NULL) {
#ifdef __cplusplus
        env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
        (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif
        h5outOfMemory(env, "H5Rget_name:  malloc failed");
        return NULL;
    }

    size = H5Rget_name((hid_t)loc_id, (H5R_type_t)ref_type, refP, rName, rname_buf_size);

    if (size < 0) {
        free(rName);
#ifdef __cplusplus
        env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
        (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif
        h5libraryError(env);
        return NULL;
    }
    if (size >= rname_buf_size) {
    		free(rName);
    		rname_buf_size = size + 1;
		    rName = (char*) malloc(sizeof(char) * rname_buf_size);
    		size = H5Rget_name((hid_t)loc_id, (H5R_type_t)ref_type, refP, rName, rname_buf_size);
        if (size < 0) {
            free(rName);
#ifdef __cplusplus
            env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
            (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif
            h5libraryError(env);
            return NULL;
        }
    }
 		rName[size] = '\0';

#ifdef __cplusplus
    env->ReleaseByteArrayElements(ref,refP,JNI_ABORT);
#else
    (*env)->ReleaseByteArrayElements(env,ref,refP,JNI_ABORT);
#endif

    /* successful return -- save the string; */
#ifdef __cplusplus
    str = env->NewStringUTF(rName);
#else
    str = (*env)->NewStringUTF(env,rName);
#endif
    free(rName);
    if (str == NULL) {
        return NULL;
    }
    return str;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    String[] H5Rget_name(hid_t id, H5R_type_t ref_type, long *_ref)
 * Signature: (I[B)Ljava/lang/String;
 */
JNIEXPORT jobjectArray JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Rget_1name__I_3J
  (JNIEnv *env, jclass clss, jint loc_id, jlongArray ref)
{
		ssize_t size;
    H5O_type_t obj_type;
    jlong *refP, *refPR;
    char *rName;
    int rname_buf_size = 128;
    jclass stringClass;
		jint arrayLen;
    jobjectArray array;
    jstring str;
    int i;

    if (ref == NULL) {
        h5nullArgument( env, "H5Rget_name:  ref is NULL");
        return NULL;
    }

#ifdef __cplusplus
    refP = (jlong *)env->GetLongArrayElements(ref, NULL);
#else
    refP = (jlong *)(*env)->GetLongArrayElements(env,ref, NULL);
#endif
    if (refP == NULL) {
        h5JNIFatalError(env,  "H5Rget_name:  ref not pinned");
        return NULL;
    }
#ifdef __cplusplus
    arrayLen = env->GetArrayLength(ref);
    stringClass = env->FindClass("java/lang/String");
    array = env->NewObjectArray(arrayLen, stringClass, NULL);
#else
    arrayLen = (*env)->GetArrayLength(env, ref);
    stringClass = (*env)->FindClass(env, "java/lang/String");
    array = (*env)->NewObjectArray(env, arrayLen, stringClass, NULL);
#endif
    if (array == NULL) {
        return NULL;
    }

    rName = (char*) malloc(sizeof(char) * rname_buf_size);
    if (rName == NULL) {
#ifdef __cplusplus
        env->ReleaseLongArrayElements(ref,refP,JNI_ABORT);
#else
        (*env)->ReleaseLongArrayElements(env,ref,refP,JNI_ABORT);
#endif
        h5outOfMemory(env, "H5Rget_name:  malloc failed");
        return NULL;
    }
    
    for (i = 0,refPR = refP; i < arrayLen; ++i,++refPR) {

        size = H5Rget_name((hid_t)loc_id, H5R_OBJECT, refPR, rName, rname_buf_size);

        if (size < 0) {
            free(rName);
#ifdef __cplusplus
            env->ReleaseLongArrayElements(ref,refP,JNI_ABORT);
#else
            (*env)->ReleaseLongArrayElements(env,ref,refP,JNI_ABORT);
#endif
            h5libraryError(env);
            return NULL;
        }
        if (size >= rname_buf_size) {
        		free(rName);
        		rname_buf_size = size + 1;
		        rName = (char*) malloc(sizeof(char) * rname_buf_size);
    		    size = H5Rget_name((hid_t)loc_id, H5R_OBJECT, refP, rName, rname_buf_size);
            if (size < 0) {
                free(rName);
#ifdef __cplusplus
                env->ReleaseLongArrayElements(ref,refP,JNI_ABORT);
#else
                (*env)->ReleaseLongArrayElements(env,ref,refP,JNI_ABORT);
#endif
                h5libraryError(env);
                return NULL;
            }
        }
 		    rName[size] = '\0';

        /* successful return -- save the string; */
#ifdef __cplusplus
        str = env->NewStringUTF(rName);
#else
        str = (*env)->NewStringUTF(env,rName);
#endif
        if (str == NULL) {
            free(rName);
#ifdef __cplusplus
            env->ReleaseLongArrayElements(ref,refP,JNI_ABORT);
#else
            (*env)->ReleaseLongArrayElements(env,ref,refP,JNI_ABORT);
#endif
            return NULL;
        }
#ifdef __cplusplus
        env->SetObjectArrayElement(array, i, str);
#else
        (*env)->SetObjectArrayElement(env, array, i, str);
#endif

    } /* for (i = 0...) */

#ifdef __cplusplus
    env->ReleaseLongArrayElements(ref,refP,JNI_ABORT);
#else
    (*env)->ReleaseLongArrayElements(env,ref,refP,JNI_ABORT);
#endif
    free(rName);

    return array;
}

#ifdef __cplusplus
}
#endif
