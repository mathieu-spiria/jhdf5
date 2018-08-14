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

/*
 * Class:     ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Lget_link_info
 */
JNIEXPORT jint JNICALL Java_ch_systemsx_cisd_hdf5_hdf5lib_HDFHelper__1H5Lget_1link_1info
  (JNIEnv *env, jclass clss, jlong loc_id, jstring object_name,
    jobjectArray linkName)
{
    jint type;
    herr_t status;
    long minor_err_num;
    char *oName;
   	char *linkval_buf;
    const char *filename;
    const char *obj_path;
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

    if (type < 0) 
    {
       (*env)->ReleaseStringUTFChars(env,object_name,oName);
       h5libraryError(env);
       return -1;
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
                H5Lunpack_elink_val(linkval_buf, link_info.u.val_size, NULL, &filename, &obj_path);
                str = (*env)->NewStringUTF(env,obj_path);
                (*env)->SetObjectArrayElement(env,linkName,0,(jobject)str);
                str = (*env)->NewStringUTF(env,filename);
                (*env)->SetObjectArrayElement(env,linkName,1,(jobject)str);
             } else /* H5L_TYPE_SYMBOLIC */
             {
                str = (*env)->NewStringUTF(env,linkval_buf);
                (*env)->SetObjectArrayElement(env,linkName,0,(jobject)str);
             }
             free(linkval_buf);
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
    char **linkfname;
    void **buf;
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
    const char *filename;
    const char *obj_path;
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
                H5Lunpack_elink_val(linkval_buf, link_info->u.val_size, NULL, &filename, &obj_path);
		        *(info->linkname+info->count) = obj_path;
		        *(info->linkfname+info->count) = filename;
		        *(info->buf+info->count) = linkval_buf;
		    } else
		    {
		        *(info->linkname+info->count) = linkval_buf;
		        *(info->linkfname+info->count) = NULL;
		        *(info->buf+info->count) = linkval_buf;
		    }
		  }
    }
    info->count++;

    return 0;
}

herr_t H5Lget_link_info_all( JNIEnv *env, hid_t loc_id, char *group_name, char **names, int *linktypes, char **linknames, char **linkfnames, void **buf )
{
    link_info_all_t info;
    info.env = env;
    info.name = names;
    info.type = linktypes;
    info.linkname = linknames;
    info.linkfname = linkfnames;
    info.buf = buf;
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
    jobjectArray oname, jintArray otype, jobjectArray lname, jobjectArray lfname, jint n)
{
    herr_t status;
    char *gName=NULL;
    char **oName=NULL;
    char **lName=NULL;
    char **lfName=NULL;
    void **buf=NULL;
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

    if ((lname != NULL && lfname == NULL) || (lname == NULL && lfname != NULL)) {
        h5nullArgument( env, "H5Lget_link_info_all:  lname and lfname either both NULL or both not NULL");
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
    for (i=0; i<n; i++) 
    {
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
	    lfName = malloc(n * sizeof (*lfName));
	    if (lfName == NULL) {
	        (*env)->ReleaseStringUTFChars(env,group_name,gName);
	        (*env)->ReleaseIntArrayElements(env,otype,tarr,0);
	        h5str_array_free(oName, n);
            free(lName);
	        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
	        return -1;
	    }
	    buf = malloc(n * sizeof (*buf));
	    if (buf == NULL) {
	        (*env)->ReleaseStringUTFChars(env,group_name,gName);
	        (*env)->ReleaseIntArrayElements(env,otype,tarr,0);
	        h5str_array_free(oName, n);
            free(lName);
            free(lfName);
	        h5outOfMemory(env, "H5Lget_link_info_all: malloc failed");
	        return -1;
	    }
	    for (i=0; i<n; i++) {
	        lName[i] = NULL;
            lfName[i] = NULL;
            buf[i] = NULL;
	    } /* for (i=0; i<n; i++)*/
	  }
    status = H5Lget_link_info_all( env, (hid_t) loc_id, gName, oName, (int *)tarr, lName, lfName, buf );

    (*env)->ReleaseStringUTFChars(env,group_name,gName);
    if (status < 0) {
        (*env)->ReleaseIntArrayElements(env,otype,tarr,JNI_ABORT);
        h5str_array_free(oName, n);
        if (lName != NULL)
        {
        	h5str_array_free(lName, n);
       	}
        if (lfName != NULL)
        {
        	h5str_array_free(lfName, n);
       	}
        if (buf != NULL)
        {
        	h5str_array_free(buf, n);
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
	        for (i=0; i<n; i++) 
            {
	            if (*(lName+i)) 
                {
	                str = (*env)->NewStringUTF(env,*(lName+i));
	                (*env)->SetObjectArrayElement(env,lname,i,(jobject)str);
	            }
	            if (*(lfName+i)) 
                {
	                str = (*env)->NewStringUTF(env,*(lfName+i));
	                (*env)->SetObjectArrayElement(env,lfname,i,(jobject)str);
	            }
                if (*(buf+i))
                {
                    /*FIXME*/
                    /* free(buf+i); */
                }
	        } /* for (i=0; i<n; i++)*/
	        free(lName);
	        free(lfName);
	        h5str_array_free(buf, n);
	      }
        h5str_array_free(oName, n);
    }

    return (jint)status;

}

