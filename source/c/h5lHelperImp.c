/*
 * Copyright 2007 - 2018 ETH Zuerich, CISD and SIS.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hdf5.h"
#include "H5Ppublic.h"

#include <jni.h>
#include <stdlib.h>
#include <string.h>

extern jboolean h5outOfMemory( JNIEnv *env, char *functName);
extern jboolean h5JNIFatalError( JNIEnv *env, char *functName);
extern jboolean h5nullArgument( JNIEnv *env, char *functName);
extern jboolean h5badArgument( JNIEnv *env, char *functName);
extern jboolean h5libraryError( JNIEnv *env );
extern void  h5str_array_free(char **strs, size_t len);

/*
/////////////////////////////////////////////////////////////////////////////////
//
// H5L and H5O helper methods.
// Add these methods so that we don't need to call H5Lget_info in a Java loop 
// to get information for all the object in a group, which takes
// a lot of time to execute if the number of objects is more than 10,000
//
/////////////////////////////////////////////////////////////////////////////////
*/

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

/* major and minor error numbers */
typedef struct H5E_num_t {
    hid_t maj_num;
    hid_t min_num;
} H5E_num_t;

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

long get_minor_error_number()
{
    H5E_num_t err_nums;
    err_nums.maj_num = 0;
    err_nums.min_num = 0;

    H5Ewalk2(H5E_DEFAULT, H5E_WALK_DOWNWARD, walk_error_callback, &err_nums);

    return (long) err_nums.min_num;
}

/*
 * Class:     ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Lget_link_info
 */
JNIEXPORT jint JNICALL Java_ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper__1H5Lget_1link_1info
  (JNIEnv *env, jclass clss, jlong loc_id, jstring object_name,
    jobjectArray linkName, jboolean exception_when_non_existent)
{
    jint type;
    herr_t status;
    long minor_err_num;
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

    oName = (*env)->GetStringUTFChars(env,object_name,&isCopy);
    if (oName == NULL) {
        h5JNIFatalError( env, "H5Lget_link_info:  object_name not pinned");
        return -1;
    }

    type = H5Lget_info( (hid_t) loc_id, oName, &link_info, H5P_DEFAULT );

    if (type < 0) {
		    (*env)->ReleaseStringUTFChars(env,object_name,oName);
        if (exception_when_non_existent == JNI_FALSE)
        {
            minor_err_num = get_minor_error_number();
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
		    	status = H5Oget_info_by_name1(loc_id, oName, &obj_info, H5P_DEFAULT); 
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

typedef struct link_info_all
{
	JNIEnv *env;
    char **name;
    int *type;
    char **linkname;
    int count;
} link_info_all_t;

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

/*
 * Class:     ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Lget_link_names_all
 */
JNIEXPORT jint JNICALL Java_ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper__1H5Lget_1link_1names_1all
  (JNIEnv *env, jclass clss, jlong loc_id, jstring group_name,
    jobjectArray oname, jint n)
{
    herr_t status;
    char *gName=NULL;
    char **oName=NULL;
    char **lName=NULL;
    jstring str;
    jboolean isCopy;
    int i;

    if (group_name == NULL) {
        h5nullArgument( env, "_H5Lget_link_names_all:  group_name is NULL");
        return -1;
    }

    if (oname == NULL) {
        h5nullArgument( env, "_H5Lget_link_names_all:  oname is NULL");
        return -1;
    }

    gName = (char *)(*env)->GetStringUTFChars(env,group_name,&isCopy);
    if (gName == NULL) {
        h5JNIFatalError( env, "_H5Lget_link_names_all:  group_name not pinned");
        return -1;
    }

    oName = malloc(n * sizeof (*oName));
    if (oName == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        h5outOfMemory(env, "_H5Lget_link_names_all: malloc failed");
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
                (*env)->SetObjectArrayElement(env,oname,i,(jobject)str);
            }
        } /* for (i=0; i<n; i++)*/
        h5str_array_free(oName, n);
    }

    return (jint)status;

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
	    if ( H5Oget_info_by_name1(loc_id, name, &obj_info, H5P_DEFAULT) < 0 )
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

/*
 * Class:     ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Lget_link_info_all
 */
JNIEXPORT jint JNICALL Java_ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper__1H5Lget_1link_1info_1all
  (JNIEnv *env, jclass clss, jlong loc_id, jstring group_name,
    jobjectArray oname, jintArray otype, jobjectArray lname, jint n)
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

    if (oname == NULL) {
        h5nullArgument( env, "H5Lget_link_info_all:  oname is NULL");
        return -1;
    }

    if (otype == NULL) {
        h5nullArgument( env, "H5Lget_link_info_all:  otype is NULL");
        return -1;
    }

    gName = (char *)(*env)->GetStringUTFChars(env,group_name,&isCopy);
    if (gName == NULL) {
        h5JNIFatalError( env, "H5Lget_link_info_all:  group_name not pinned");
        return -1;
    }

    tarr = (*env)->GetIntArrayElements(env,otype,&isCopy);
    if (tarr == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        h5JNIFatalError( env, "H5Lget_link_info_all:  type not pinned");
        return -1;
    }

    oName = malloc(n * sizeof (*oName));
    if (oName == NULL) {
        (*env)->ReleaseStringUTFChars(env,group_name,gName);
        (*env)->ReleaseIntArrayElements(env,otype,tarr,0);
        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
        return -1;
    }
    for (i=0; i<n; i++) {
        oName[i] = NULL;
    } /* for (i=0; i<n; i++)*/
    if (lname != NULL)
    {
	    lName = malloc(n * sizeof (*lName));
	    if (lName == NULL) {
	        (*env)->ReleaseStringUTFChars(env,group_name,gName);
	        (*env)->ReleaseIntArrayElements(env,otype,tarr,0);
	        h5str_array_free(oName, n);
	        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
	        return -1;
	    }
	    for (i=0; i<n; i++) {
	        lName[i] = NULL;
	    } /* for (i=0; i<n; i++)*/
	  }
    status = H5Lget_link_info_all( env, (hid_t) loc_id, gName, oName, (int *)tarr, lName );

    (*env)->ReleaseStringUTFChars(env,group_name,gName);
    if (status < 0) {
        (*env)->ReleaseIntArrayElements(env,otype,tarr,JNI_ABORT);
        h5str_array_free(oName, n);
        if (lName != NULL)
        {
        	h5str_array_free(lName, n);
       	}
        h5libraryError(env);
    } else {
        (*env)->ReleaseIntArrayElements(env,otype,tarr,0);

        for (i=0; i<n; i++) {
            if (*(oName+i)) {
                str = (*env)->NewStringUTF(env,*(oName+i));
                (*env)->SetObjectArrayElement(env,oname,i,(jobject)str);
            }
        } /* for (i=0; i<n; i++)*/
        if (lname != NULL)
        {
	        for (i=0; i<n; i++) {
	            if (*(lName+i)) {
	                str = (*env)->NewStringUTF(env,*(lName+i));
	                (*env)->SetObjectArrayElement(env,lname,i,(jobject)str);
	            }
	        } /* for (i=0; i<n; i++)*/
	        h5str_array_free(lName, n);
	      }
        h5str_array_free(oName, n);
    }

    return (jint)status;

}

