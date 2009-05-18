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
#include "h5util.h"
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
#include <string.h>

extern jboolean h5outOfMemory( JNIEnv *env, char *functName);
extern jboolean h5JNIFatalError( JNIEnv *env, char *functName);
extern jboolean h5nullArgument( JNIEnv *env, char *functName);
extern jboolean h5badArgument( JNIEnv *env, char *functName);
extern jboolean h5libraryError( JNIEnv *env );
extern int getMinorErrorNumber();

#ifdef __cplusplus
herr_t link_info_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata);
herr_t H5Lget_link_info_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names, int *type, char **linknames );
herr_t link_names_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata);
herr_t H5Lget_link_names_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names );
#else
static herr_t link_info_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata);
static herr_t H5Lget_link_info_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names, int *type, char **linknames );
static herr_t link_names_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata);
static herr_t H5Lget_link_names_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names );
#endif

typedef struct link_info_all
{
	JNIEnv *env;
    char **name;
    int *type;
    char **linkname;
    int count;
} link_info_all_t;

char *get_external_link(  JNIEnv *env, const char *linkval_buf, size_t size ) {
      const char *filename;
      const char *obj_path;
      char *external_link_buf;
      const char *prefix = "EXTERNAL::";
      H5Lunpack_elink_val(linkval_buf, size, NULL, &filename, &obj_path);
      external_link_buf = (char *) malloc(strlen(prefix) + strlen(filename) + strlen(obj_path) + 3);
      if (external_link_buf == NULL)
      {
	        h5outOfMemory(env, "get_external_link: malloc failed");
	        return NULL;
      }
      strcpy(external_link_buf, prefix);
      strcat(external_link_buf, filename);
      strcat(external_link_buf, "::");
      strcat(external_link_buf, obj_path);
      return external_link_buf;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lcreate_hard
 * Signature: (ILjava/lang/String;ILjava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lcreate_1hard
  (JNIEnv *env, jclass clss, jint obj_loc_id, jstring
    obj_name, jint link_loc_id, jstring link_name, jint lcpl_id, jint lapl_id)
{
    herr_t status;
    char *oName, *lName;
    jboolean isCopy;

    if (obj_name == NULL) {
        h5nullArgument( env, "H5Lcreate_hard:  obj_name is NULL");
        return -1;
    }
    if (link_name == NULL) {
        h5nullArgument( env, "H5Lcreate_hard:  link_name is NULL");
        return -1;
    }
#ifdef __cplusplus
    oName = (char *)env->GetStringUTFChars(obj_name,&isCopy);
#else
    oName = (char *)(*env)->GetStringUTFChars(env,obj_name,&isCopy);
#endif
    if (oName == NULL) {
        h5JNIFatalError( env, "H5Lcreate_hard:  obj_name not pinned");
        return -1;
    }
#ifdef __cplusplus
    lName = (char *)env->GetStringUTFChars(link_name,&isCopy);
#else
    lName = (char *)(*env)->GetStringUTFChars(env,link_name,&isCopy);
#endif
    if (lName == NULL) {
#ifdef __cplusplus
        env->ReleaseStringUTFChars(obj_name,oName);
#else
        (*env)->ReleaseStringUTFChars(env,obj_name,oName);
#endif
        h5JNIFatalError( env, "H5Lcreate_hard:  link_name not pinned");
        return -1;
    }

    status = H5Lcreate_hard((hid_t)obj_loc_id, oName, (hid_t)link_loc_id, lName, lcpl_id, lapl_id);

#ifdef __cplusplus
    env->ReleaseStringUTFChars(link_name,lName);
    env->ReleaseStringUTFChars(obj_name,oName);
#else
    (*env)->ReleaseStringUTFChars(env,link_name,lName);
    (*env)->ReleaseStringUTFChars(env,obj_name,oName);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lcreate_soft
 * Signature: (Ljava/lang/String;ILjava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lcreate_1soft
  (JNIEnv *env, jclass clss, jstring target_path, 
  	jint link_loc_id, jstring link_name, jint lcpl_id, jint lapl_id)
{
    herr_t status;
    char *tPath, *lName;
    jboolean isCopy;

    if (target_path == NULL) {
        h5nullArgument( env, "H5Lcreate_soft:  target_path is NULL");
        return -1;
    }
    if (link_name == NULL) {
        h5nullArgument( env, "H5Lcreate_soft:  link_name is NULL");
        return -1;
    }
#ifdef __cplusplus
    tPath = (char *)env->GetStringUTFChars(target_path,&isCopy);
#else
    tPath = (char *)(*env)->GetStringUTFChars(env,target_path,&isCopy);
#endif
    if (tPath == NULL) {
        h5JNIFatalError( env, "H5Lcreate_soft:  target_path not pinned");
        return -1;
    }
#ifdef __cplusplus
    lName = (char *)env->GetStringUTFChars(link_name,&isCopy);
#else
    lName = (char *)(*env)->GetStringUTFChars(env,link_name,&isCopy);
#endif
    if (lName == NULL) {
#ifdef __cplusplus
        env->ReleaseStringUTFChars(target_path,tPath);
#else
        (*env)->ReleaseStringUTFChars(env,target_path,tPath);
#endif
        h5JNIFatalError( env, "H5Lcreate_soft:  link_name not pinned");
        return -1;
    }

    status = H5Lcreate_soft(tPath, (hid_t)link_loc_id, lName, lcpl_id, lapl_id);

#ifdef __cplusplus
    env->ReleaseStringUTFChars(link_name,lName);
    env->ReleaseStringUTFChars(target_path,tPath);
#else
    (*env)->ReleaseStringUTFChars(env,link_name,lName);
    (*env)->ReleaseStringUTFChars(env,target_path,tPath);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lcreate_external
 * Signature: (Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lcreate_1external
  (JNIEnv *env, jclass clss, jstring file_name, jstring
    obj_name, jint link_loc_id, jstring link_name, jint lcpl_id, jint lapl_id)
{
    herr_t status;
    char *fName, *oName, *lName;
    jboolean isCopy;

    if (file_name == NULL) {
        h5nullArgument( env, "H5Lcreate_external:  file_name is NULL");
        return -1;
    }
    if (obj_name == NULL) {
        h5nullArgument( env, "H5Lcreate_external:  obj_name is NULL");
        return -1;
    }
    if (link_name == NULL) {
        h5nullArgument( env, "H5Lcreate_external:  link_name is NULL");
        return -1;
    }
#ifdef __cplusplus
    fName = (char *)env->GetStringUTFChars(file_name,&isCopy);
#else
    fName = (char *)(*env)->GetStringUTFChars(env,file_name,&isCopy);
#endif
    if (fName == NULL) {
        h5JNIFatalError( env, "H5Lcreate_external:  file_name not pinned");
        return -1;
    }
#ifdef __cplusplus
    oName = (char *)env->GetStringUTFChars(obj_name,&isCopy);
#else
    oName = (char *)(*env)->GetStringUTFChars(env,obj_name,&isCopy);
#endif
    if (oName == NULL) {
#ifdef __cplusplus
        env->ReleaseStringUTFChars(file_name,fName);
#else
        (*env)->ReleaseStringUTFChars(env,file_name,fName);
#endif
        h5JNIFatalError( env, "H5Lcreate_external:  obj_name not pinned");
        return -1;
    }
#ifdef __cplusplus
    lName = (char *)env->GetStringUTFChars(link_name,&isCopy);
#else
    lName = (char *)(*env)->GetStringUTFChars(env,link_name,&isCopy);
#endif
    if (lName == NULL) {
#ifdef __cplusplus
        env->ReleaseStringUTFChars(file_name,fName);
        env->ReleaseStringUTFChars(obj_name,oName);
#else
        (*env)->ReleaseStringUTFChars(env,file_name,fName);
        (*env)->ReleaseStringUTFChars(env,obj_name,oName);
#endif
        h5JNIFatalError( env, "H5Lcreate_external:  link_name not pinned");
        return -1;
    }

    status = H5Lcreate_external(fName, oName, (hid_t)link_loc_id, lName, lcpl_id, lapl_id);

#ifdef __cplusplus
    env->ReleaseStringUTFChars(file_name,fName);
    env->ReleaseStringUTFChars(link_name,lName);
    env->ReleaseStringUTFChars(obj_name,oName);
#else
    (*env)->ReleaseStringUTFChars(env,file_name,fName);
    (*env)->ReleaseStringUTFChars(env,link_name,lName);
    (*env)->ReleaseStringUTFChars(env,obj_name,oName);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lmove
 * Signature: (ILjava/lang/String;ILjava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lmove
  (JNIEnv *env, jclass clss, jint src_loc_id, jstring
    src_name, jint dest_loc_id, jstring dest_name, jint lcpl_id, jint lapl_id)
{
    herr_t status;
    char *srcName, *dstName;
    jboolean isCopy;

    if (src_name == NULL) {
        h5nullArgument( env, "H5Lmove:  src_name is NULL");
        return -1;
    }
    if (dest_name == NULL) {
        h5nullArgument( env, "H5Lmove:  dest_name is NULL");
        return -1;
    }
#ifdef __cplusplus
    srcName = (char *)env->GetStringUTFChars(src_name,&isCopy);
#else
    srcName = (char *)(*env)->GetStringUTFChars(env,src_name,&isCopy);
#endif
    if (srcName == NULL) {
        h5JNIFatalError( env, "H5Lmove:  src_name not pinned");
        return -1;
    }
#ifdef __cplusplus
    dstName = (char *)env->GetStringUTFChars(dest_name,&isCopy);
#else
    dstName = (char *)(*env)->GetStringUTFChars(env,dest_name,&isCopy);
#endif
    if (dstName == NULL) {
#ifdef __cplusplus
        env->ReleaseStringUTFChars(src_name,srcName);
#else
        (*env)->ReleaseStringUTFChars(env,src_name,srcName);
#endif
        h5JNIFatalError( env, "H5Lmove:  dest_name not pinned");
        return -1;
    }

    status = H5Lmove((hid_t)src_loc_id, srcName, (hid_t)dest_loc_id, dstName, lcpl_id, lapl_id);

#ifdef __cplusplus
    env->ReleaseStringUTFChars(dest_name,dstName);
    env->ReleaseStringUTFChars(src_name,srcName);
#else
    (*env)->ReleaseStringUTFChars(env,dest_name,dstName);
    (*env)->ReleaseStringUTFChars(env,src_name,srcName);
#endif

    if (status < 0) {
        h5libraryError(env);
    }
    return (jint)status;
}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lget_link_info
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lget_1link_1info
  (JNIEnv *env, jclass clss, jint loc_id, jstring object_name,
    jobjectArray linkName, jboolean exception_when_non_existent)
{
    jint type;
    herr_t status;
    int minor_err_num;
    char *oName;
   	char *linkval_buf;
   	char *linkname_buf;
    jboolean isCopy;
    jstring str;
    H5L_info_t link_info;
    H5O_info_t obj_info;

    if (object_name == NULL) {
        h5nullArgument( env, "H5Lget_link_info:  object_name is NULL");
        return -1;
    }

#ifdef __cplusplus
    oName = (char *)env->GetStringUTFChars(object_name,&isCopy);
#else
    oName = (char *)(*env)->GetStringUTFChars(env,object_name,&isCopy);
#endif
    if (oName == NULL) {
        h5JNIFatalError( env, "H5Lget_link_info:  object_name not pinned");
        return -1;
    }

    type = H5Lget_info( (hid_t) loc_id, oName, &link_info, H5P_DEFAULT );

    if (type < 0) {
#ifdef __cplusplus
		    env->ReleaseStringUTFChars(object_name,oName);
#else
		    (*env)->ReleaseStringUTFChars(env,object_name,oName);
#endif
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
    } else {
		    str = NULL;
		    if (link_info.type == H5L_TYPE_HARD)
		    {
		    	status = H5Oget_info_by_name(loc_id, oName, &obj_info, H5P_DEFAULT); 
			    (*env)->ReleaseStringUTFChars(env,object_name,oName);
			    if (status  < 0 )
			    {
		        h5libraryError(env);
		        return -1;
			    } else {
			        type = obj_info.type;
			    }
			  } else
			  {
		      type = H5O_TYPE_NTYPES + link_info.type;
		      if (linkName != NULL)
		      {
			    	linkval_buf = (char*) malloc(link_info.u.val_size);
				    if (linkval_buf == NULL)
				    {
				        h5outOfMemory(env, "H5Lget_link_info: malloc failed");
			  	      return -1;
			    	}
				    if (H5Lget_val(loc_id, oName, linkval_buf, link_info.u.val_size, H5P_DEFAULT) < 0)
				    {
			        h5libraryError(env);
							return -1;					
				    }
				    if (link_info.type == H5L_TYPE_EXTERNAL)
				    {
				        linkname_buf = get_external_link(env, linkval_buf, link_info.u.val_size);
				        free(linkval_buf);
			  	  } else
			    	{
			      	  linkname_buf = linkval_buf;
			    	}
            str = (*env)->NewStringUTF(env,linkname_buf);
	          (*env)->SetObjectArrayElement(env,linkName,0,(jobject)str);
	        }
	      }
    }

    return (jint)type;

}

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lexists
 */
JNIEXPORT jboolean JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lexists
  (JNIEnv *env, jclass clss, jint loc_id, jstring link_name)
{
    htri_t exists;
    char *lName;
    jboolean isCopy;

    if (link_name == NULL) {
        h5nullArgument( env, "H5Lexists:  link_name is NULL");
        return -1;
    }

    lName = (char *)(*env)->GetStringUTFChars(env,link_name,&isCopy);
    if (lName == NULL) {
        h5JNIFatalError( env, "H5Lexists:  link_name not pinned");
        return -1;
    }

    exists = H5Lexists( (hid_t) loc_id, lName, H5P_DEFAULT );
    if (exists < 0)
    {
        if (getMinorErrorNumber() == H5E_NOTFOUND)
        {
            exists = 0;
        } else
        {
            h5libraryError(env);
        }
    }

    (*env)->ReleaseStringUTFChars(env,link_name,lName);
    
    return exists;
}

/*
/////////////////////////////////////////////////////////////////////////////////
//
//
// Add these methods so that we don't need to call H5Lget_info
// in a loop to get information for all the object in a group, which takes
// a lot of time to finish if the number of objects is more than 10,000
//
/////////////////////////////////////////////////////////////////////////////////
*/

/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lget_link_names_all
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lget_1link_1names_1all
  (JNIEnv *env, jclass clss, jint loc_id, jstring group_name,
    jobjectArray objName, jint n)
{
    herr_t status;
    char *gName=NULL;
    char **oName=NULL;
    char **lName=NULL;
    jboolean isCopy;
    jstring str;
    jint *tarr;
    int i;

    if (group_name == NULL) {
        h5nullArgument( env, "H5Lget_link_info_all:  group_name is NULL");
        return -1;
    }

    gName = (char *)(*env)->GetStringUTFChars(env,group_name,&isCopy);
    if (gName == NULL) {
        h5JNIFatalError( env, "H5Lget_link_info_all:  group_name not pinned");
        return -1;
    }

    oName = malloc(n * sizeof (*oName));
    if (oName == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
        return -1;
    }
    for (i=0; i<n; i++) {
        oName[i] = NULL;
    } /* for (i=0; i<n; i++)*/
    status = H5Lget_link_names_all(env, (hid_t) loc_id, gName,  oName);

    (*env)->ReleaseStringUTFChars(env,group_name,gName);
    if (status < 0) {
        h5str_array_free(oName, n);
        h5libraryError(env);
    } else {
        for (i=0; i<n; i++) {
            if (*(oName+i)) {
                str = (*env)->NewStringUTF(env,*(oName+i));
                (*env)->SetObjectArrayElement(env,objName,i,(jobject)str);
            }
        } /* for (i=0; i<n; i++)*/
        h5str_array_free(oName, n);
    }

    return (jint)status;

}

herr_t H5Lget_link_names_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names )
{
    link_info_all_t info;
    info.env = env;
    info.name = names;
    info.count = 0;

    if(H5Literate_by_name(loc_id, group_name, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, link_names_all, (void *)&info, H5P_DEFAULT) < 0)
        return -1;

    return 0;
}

herr_t link_names_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata)
{
    link_info_all_t* info = (link_info_all_t*)opdata;
    H5O_info_t obj_info;
    
    *(info->name+info->count) = (char *) malloc(strlen(name)+1);
    if (*(info->name+info->count) == NULL)
    {
        h5outOfMemory(info->env, "H5Lget_link_info_all: malloc failed");
        return -1;
    }
    strcpy(*(info->name+info->count), name);
    
    info->count++;

    return 0;
}
/*
 * Class:     ncsa_hdf_hdf5lib_H5
 * Method:    H5Lget_link_info_all
 */
JNIEXPORT jint JNICALL Java_ncsa_hdf_hdf5lib_H5_H5Lget_1link_1info_1all
  (JNIEnv *env, jclass clss, jint loc_id, jstring group_name,
    jobjectArray objName, jintArray oType, jobjectArray linkName, jint n)
{
    herr_t status;
    char *gName=NULL;
    char **oName=NULL;
    char **lName=NULL;
    jboolean isCopy;
    jstring str;
    jint *tarr;
    int i;

    if (group_name == NULL) {
        h5nullArgument( env, "H5Lget_link_info_all:  group_name is NULL");
        return -1;
    }

    if (oType == NULL) {
        h5nullArgument( env, "H5Lget_link_info_all:  oType is NULL");
        return -1;
    }

    gName = (char *)(*env)->GetStringUTFChars(env,group_name,&isCopy);
    if (gName == NULL) {
        h5JNIFatalError( env, "H5Lget_link_info_all:  group_name not pinned");
        return -1;
    }

    tarr = (*env)->GetIntArrayElements(env,oType,&isCopy);
    if (tarr == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        h5JNIFatalError( env, "H5Lget_link_info_all:  type not pinned");
        return -1;
    }

    oName = malloc(n * sizeof (*oName));
    if (oName == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        (*env)->ReleaseIntArrayElements(env,oType,tarr,0);
        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
        return -1;
    }
    for (i=0; i<n; i++) {
        oName[i] = NULL;
    } /* for (i=0; i<n; i++)*/
    if (linkName != NULL)
    {
	    lName = malloc(n * sizeof (*lName));
	    if (lName == NULL) {
	        (*env)->ReleaseStringUTFChars(env,group_name,gName);
	        (*env)->ReleaseIntArrayElements(env,oType,tarr,0);
	        h5str_array_free(oName, n);
	        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
	        return -1;
	    }
	    for (i=0; i<n; i++) {
	        lName[i] = NULL;
	    } /* for (i=0; i<n; i++)*/
	  }
    status = H5Lget_link_info_all( env, (hid_t) loc_id, gName,  oName, (int *)tarr, lName );

    (*env)->ReleaseStringUTFChars(env,group_name,gName);
    if (status < 0) {
        (*env)->ReleaseIntArrayElements(env,oType,tarr,JNI_ABORT);
        h5str_array_free(oName, n);
        if (lName != NULL)
        {
        	h5str_array_free(lName, n);
       	}
        h5libraryError(env);
    } else {
        (*env)->ReleaseIntArrayElements(env,oType,tarr,0);

        for (i=0; i<n; i++) {
            if (*(oName+i)) {
                str = (*env)->NewStringUTF(env,*(oName+i));
                (*env)->SetObjectArrayElement(env,objName,i,(jobject)str);
            }
        } /* for (i=0; i<n; i++)*/
        if (linkName != NULL)
        {
	        for (i=0; i<n; i++) {
	            if (*(lName+i)) {
	                str = (*env)->NewStringUTF(env,*(lName+i));
	                (*env)->SetObjectArrayElement(env,linkName,i,(jobject)str);
	            }
	        } /* for (i=0; i<n; i++)*/
	        h5str_array_free(lName, n);
	      }
        h5str_array_free(oName, n);
    }

    return (jint)status;

}

herr_t H5Lget_link_info_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names, int *linktypes, char **linknames )
{
    link_info_all_t info;
    info.env = env;
    info.name = names;
    info.type = linktypes;
    info.linkname = linknames;
    info.count = 0;

    if(H5Literate_by_name(loc_id, group_name, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, link_info_all, (void *)&info, H5P_DEFAULT) < 0)
        return -1;

    return 0;
}

herr_t link_info_all(hid_t loc_id, const char *name, const H5L_info_t *link_info, void *opdata)
{
    link_info_all_t* info = (link_info_all_t*)opdata;
    H5O_info_t obj_info;
   	char *linkval_buf;
    *(info->name+info->count) = (char *) malloc(strlen(name)+1);
    if (*(info->name+info->count) == NULL)
    {
        h5outOfMemory(info->env, "H5Lget_link_info_all: malloc failed");
        return -1;
    }
    strcpy(*(info->name+info->count), name);
    
    if (link_info->type == H5L_TYPE_HARD)
    {
      if (info->linkname != NULL)
      {
	    	*(info->linkname+info->count) = NULL;
	    	}
	    if ( H5Oget_info_by_name(loc_id, name, &obj_info, H5P_DEFAULT) < 0 )
	    {
	        *(info->type+info->count) = H5O_TYPE_UNKNOWN;
	    } else {
	        *(info->type+info->count) = obj_info.type;
	    }
	  } else
	  {
      *(info->type+info->count) = H5O_TYPE_NTYPES + link_info->type;
      if (info->linkname != NULL)
      {
	    	linkval_buf = (char*) malloc(link_info->u.val_size);
		    if (linkval_buf == NULL)
		    {
		        h5outOfMemory(info->env, "H5Lget_link_info_all: malloc failed");
		        return -1;
		    }
		    if (H5Lget_val(loc_id, name, linkval_buf, link_info->u.val_size, H5P_DEFAULT) < 0)
		    {
	        h5libraryError(info->env);
	        free(linkval_buf);
					return -1;	        
		    }
		    if (link_info->type == H5L_TYPE_EXTERNAL)
		    {
		        *(info->linkname+info->count) = get_external_link( info->env, linkval_buf, link_info->u.val_size );
		        free(linkval_buf);
		    } else
		    {
		        *(info->linkname+info->count) = linkval_buf;
		    }
		  }
    }
    info->count++;

    return 0;
}
