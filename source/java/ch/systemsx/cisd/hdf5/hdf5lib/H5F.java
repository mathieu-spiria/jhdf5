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
 * Low-level interface for HDF5 file functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5F
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Fopen opens an existing file and is the primary function for accessing existing HDF5 files.
     * 
     * @param name Name of the file to access.
     * @param flags File access flags.
     * @param access_id Identifier for the file access properties list.
     * @return a file identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Fopen(String name, int flags, int access_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fopen(name, flags, access_id);
        }
    }

    /**
     * H5Fcreate is the primary function for creating HDF5 files.
     * 
     * @param name Name of the file to access.
     * @param flags File access flags. Possible values include:
     *            <UL>
     *            <LI>H5F_ACC_RDWR Allow read and write access to file.</LI>
     *            <LI>H5F_ACC_RDONLY Allow read-only access to file.</LI>
     *            <LI>H5F_ACC_TRUNC Truncate file, if it already exists, erasing all data previously
     *            stored in the file.</LI>
     *            <LI>H5F_ACC_EXCL Fail if file already exists.</LI>
     *            <LI>H5F_ACC_DEBUG Print debug information.</LI>
     *            <LI>H5P_DEFAULT Apply default file access and creation properties.</LI>
     *            </UL>
     * @param create_id File creation property list identifier, used when modifying default file
     *            meta-data. Use H5P_DEFAULT for default access properties.
     * @param access_id File access property list identifier. If parallel file access is desired,
     *            this is a collective call according to the communicator stored in the access_id
     *            (not supported in Java). Use H5P_DEFAULT for default access properties.
     * @return a file identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Fcreate(String name, int flags, int create_id, int access_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fcreate(name, flags, create_id, access_id);
        }
    }

    /**
     * H5Fflush causes all buffers associated with a file or object to be immediately flushed
     * (written) to disk without removing the data from the (memory) cache.
     * <P>
     * After this call completes, the file (or object) is in a consistent state and all data written
     * to date is assured to be permanent.
     * 
     * @param object_id Identifier of object used to identify the file. <b>object_id</b> can be any
     *            object associated with the file, including the file itself, a dataset, a group, an
     *            attribute, or a named data type.
     * @param scope specifies the scope of the flushing action, in the case that the HDF-5 file is
     *            not a single physical file.
     *            <P>
     *            Valid values are:
     *            <UL>
     *            <LI>H5F_SCOPE_GLOBAL Flushes the entire virtual file.</LI>
     *            <LI>H5F_SCOPE_LOCAL Flushes only the specified file.</LI>
     *            </UL>
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Fflush(int object_id, int scope) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fflush(object_id, scope);
        }
    }

    /**
     * H5Fis_hdf5 determines whether a file is in the HDF5 format.
     * 
     * @param name File name to check format.
     * @return true if is HDF-5, false if not.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static boolean H5Fis_hdf5(String name) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fis_hdf5(name);
        }
    }

    /**
     * H5Fget_create_plist returns a file creation property list identifier identifying the creation
     * properties used to create this file.
     * 
     * @param file_id Identifier of the file to get creation property list
     * @return a file creation property list identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Fget_create_plist(int file_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_create_plist(file_id);
        }
    }

    /**
     * H5Fget_access_plist returns the file access property list identifier of the specified file.
     * 
     * @param file_id Identifier of file to get access property list of
     * @return a file access property list identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Fget_access_plist(int file_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_access_plist(file_id);
        }
    }

    /**
     * H5Fclose terminates access to an HDF5 file.
     * 
     * @param file_id Identifier of a file to terminate access to.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Fclose(int file_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fclose(file_id);
        }
    }

    /**
     * H5Fmount mounts the file specified by child_id onto the group specified by loc_id and name
     * using the mount properties plist_id.
     * 
     * @param loc_id The identifier for the group onto which the file specified by child_id is to be
     *            mounted.
     * @param name The name of the group onto which the file specified by child_id is to be mounted.
     * @param child_id The identifier of the file to be mounted.
     * @param plist_id The identifier of the property list to be used.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Fmount(int loc_id, String name, int child_id, int plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fmount(loc_id, name, child_id, plist_id);
        }
    }

    /**
     * Given a mount point, H5Funmount dissassociates the mount point's file from the file mounted
     * there.
     * 
     * @param loc_id The identifier for the location at which the specified file is to be unmounted.
     * @param name The name of the file to be unmounted.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Funmount(int loc_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Funmount(loc_id, name);
        }
    }

    /**
     * H5Freopen reopens an HDF5 file.
     * 
     * @param file_id Identifier of a file to terminate and reopen access to.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @return a new file identifier if successful
     */
    public static int H5Freopen(int file_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Freopen(file_id);
        }
    }

    public static int H5Fget_obj_ids(int file_id, int types, int max, int[] obj_id_list)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_obj_ids(file_id, types, max, obj_id_list);
        }
    }

    public static int H5Fget_obj_count(int file_id, int types) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_obj_count(file_id, types);
        }
    }

    public static long H5Fget_name(int obj_id, String name, int size) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_name(obj_id, name, size);
        }
    }

    public static long H5Fget_filesize(int file_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Fget_filesize(file_id);
        }
    }
}
