/*
 * Copyright 2007 - 2014 ETH Zuerich, CISD and SIS.
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

package ch.systemsx.cisd.hdf5.hdf5lib;

import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Low-level interface for HDF5 general functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5General
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /** Call to ensure that the native library is loaded. */
    public static void ensureNativeLibIsLoaded()
    {
        H5.ensureNativeLibIsLoaded();
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5: General Library Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5open initialize the library.
     * 
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5open() throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5open();
        }
    }

    /**
     * H5get_libversion retrieves the major, minor, and release numbers of the version of the HDF
     * library which is linked to the application.
     * 
     * @param libversion The version information of the HDF library.
     * 
     *            <pre>
     * 
     *            libversion[0] = The major version of the library. libversion[1] = The minor
     *            version of the library. libversion[2] = The release number of the library.
     * 
     * </pre>
     * @return a non-negative value if successful, along with the version information.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5get_libversion(int[] libversion) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5get_libversion(libversion);
        }
    }

    public static int H5set_free_list_limits(int reg_global_lim, int reg_list_lim,
            int arr_global_lim, int arr_list_lim, int blk_global_lim, int blk_list_lim)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5set_free_list_limits(reg_global_lim, reg_list_lim, arr_global_lim,
                    arr_list_lim, blk_global_lim, blk_list_lim);
        }
    }

    public static int H5Zfilter_avail(int filter) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Zfilter_avail(filter);
        }
    }

    public static int H5Zunregister(int filter) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Zunregister(filter);
        }
    }

    public static int H5Zget_filter_info(int filter) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Zget_filter_info(filter);
        }
    }

}
