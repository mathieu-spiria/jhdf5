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

extern jboolean h5outOfMemory( JNIEnv *env, char *functName);
extern jboolean h5JNIFatalError( JNIEnv *env, char *functName);
extern jboolean h5nullArgument( JNIEnv *env, char *functName);
extern jboolean h5libraryError( JNIEnv *env );

H5T_conv_ret_t abort_on_overflow_cb(int except_type, hid_t *src_id, hid_t *dst_id, void *src_buf, void *dst_buf, void *op_data)
{
    if (except_type == H5T_CONV_EXCEPT_RANGE_HI || except_type == H5T_CONV_EXCEPT_RANGE_LOW)
    {
        return H5T_CONV_ABORT;
    }
    return H5T_CONV_UNHANDLED;
}

/*
 * Class:     ch_ethz_sis_cisd_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Pcreate_xfer_abort_overflow
 * Signature: hid_t _H5Pcreate_xfer_abort_overflow()
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_sis_hdf5_hdf5lib_HDFHelper__1H5Pcreate_1xfer_1abort_1overflow
  (JNIEnv *env, jclass clss)
{
    hid_t plist;
    herr_t status;

    plist = H5Pcreate(H5P_DATASET_XFER);
    if (plist < 0) {
        h5libraryError(env);
    }
    status = H5Pset_type_conv_cb(plist, (H5T_conv_except_func_t) abort_on_overflow_cb, NULL);
    if (status < 0)
    {
        h5libraryError(env);
    }
    return plist;
}

H5T_conv_ret_t abort_cb(int except_type, hid_t *src_id, hid_t *dst_id, void *src_buf, void *dst_buf, void *op_data)
{
    return H5T_CONV_ABORT;
}

/*
 * Class:     ch_ethz_sis_hdf5_hdf5lib_HDFHelper
 * Method:    _H5Pcreate_xfer_abort
 * Signature: hid_t _H5Pcreate_xfer_abort()
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_sis_hdf5_hdf5lib_HDFHelper__1H5Pcreate_1xfer_1abort
  (JNIEnv *env, jclass clss)
{
    hid_t plist;
    herr_t status;

    plist = H5Pcreate(H5P_DATASET_XFER);
    if (plist < 0) {
        h5libraryError(env);
    }
    status = H5Pset_type_conv_cb(plist, (H5T_conv_except_func_t) abort_cb, NULL);
    if (status < 0)
    {
        h5libraryError(env);
    }
    return plist;
}

