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
 * Low-level interface for HDF5 group, link and object functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5GLO
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5G: Group Interface Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5Gcreate creates a new group with the specified name at the specified location, loc_id.
     * 
     * @param loc_id The file or group identifier.
     * @param name The absolute or relative name of the new group.
     * @param link_create_plist_id Property list for link creation.
     * @param group_create_plist_id Property list for group creation.
     * @param group_access_plist_id Property list for group access.
     * @return a valid group identifier for the open group if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Gcreate(int loc_id, String name, int link_create_plist_id,
            int group_create_plist_id, int group_access_plist_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gcreate(loc_id, name, link_create_plist_id, group_create_plist_id,
                    group_access_plist_id);
        }
    }

    /**
     * H5Gopen opens an existing group with the specified name at the specified location, loc_id.
     * 
     * @param loc_id File or group identifier within which group is to be open.
     * @param name Name of group to open.
     * @param access_plist_id Group access property list identifier (H5P_DEFAULT for the default
     *            property list).
     * @return a valid group identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Gopen(int loc_id, String name, int access_plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gopen(loc_id, name, access_plist_id);
        }
    }

    /**
     * H5Gclose releases resources used by a group which was opened by a call to H5Gcreate() or
     * H5Gopen().
     * 
     * @param group_id Group identifier to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Gclose(int group_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gclose(group_id);
        }
    }

    /**
     * H5Gunlink removes an association between a name and an object.
     * 
     * @param loc_id Identifier of the file containing the object.
     * @param name Name of the object to unlink.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Gunlink(int loc_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gunlink(loc_id, name);
        }
    }

    /**
     * H5Gset_comment sets the comment for the the object name to comment. Any previously existing
     * comment is overwritten.
     * 
     * @param loc_id IN: Identifier of the file, group, dataset, or datatype.
     * @param name IN: Name of the object whose comment is to be set or reset.
     * @param comment IN: The new comment.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name or comment is null.
     */
    public static int H5Gset_comment(int loc_id, String name, String comment)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gset_comment(loc_id, name, comment);
        }
    }

    /**
     * H5Gget_comment retrieves the comment for the the object name. The comment is returned in the
     * buffer comment.
     * 
     * @param loc_id IN: Identifier of the file, group, dataset, or datatype.
     * @param name IN: Name of the object whose comment is to be set or reset.
     * @param bufsize IN: Anticipated size of the buffer required to hold comment.
     * @param comment OUT: The comment.
     * @return the number of characters in the comment, counting the null terminator, if successful
     * @exception ArrayIndexOutOfBoundsException - JNI error writing back data
     * @exception ArrayStoreException - JNI error writing back data
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     * @exception IllegalArgumentException - size < 1, comment is invalid.
     */
    public static int H5Gget_comment(int loc_id, String name, int bufsize, String[] comment)
            throws ArrayIndexOutOfBoundsException, ArrayStoreException, HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gget_comment(loc_id, name, bufsize, comment);
        }
    }

    /**
     * Returns the number of links in the group specified by group_id.
     * 
     * @param group_id Group identifier.
     * @return Return the number of link in the group if successful.
     */
    public static long H5Gget_nlinks(int group_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Gget_nlinks(group_id);
        }
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5G: Object Interface Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5Oopen opens an existing object with the specified name at the specified location, loc_id.
     * 
     * @param loc_id File or group identifier within which object is to be open.
     * @param name Name of object to open.
     * @param access_plist_id Object access property list identifier (H5P_DEFAULT for the default
     *            property list).
     * @return a valid object identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Oopen(int loc_id, String name, int access_plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Oopen(loc_id, name, access_plist_id);
        }
    }

    /**
     * H5Oclose releases resources used by an object which was opened by a call to H5Oopen().
     * 
     * @param loc_id Object identifier to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Oclose(int loc_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Oclose(loc_id);
        }
    }

    /**
     * H5Ocopy copies an existing object with the specified src_name at the specified location,
     * src_loc_id, to the specified dst_name at the specified destination location, dst_loc_id.
     * 
     * @param src_loc_id Source File or group identifier within which object is to be open.
     * @param src_name Name of source object to open.
     * @param dst_loc_id Destination File or group identifier within which object is to be open.
     * @param dst_name Name of destination object to open.
     * @param object_copy_plist Object copy property list identifier (H5P_DEFAULT for the default
     *            property list).
     * @param link_creation_plist Link creation property list identifier for the new hard link
     *            (H5P_DEFAULT for the default property list).
     * @return a valid object identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Ocopy(int src_loc_id, String src_name, int dst_loc_id, String dst_name,
            int object_copy_plist, int link_creation_plist) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Ocopy(src_loc_id, src_name, dst_loc_id, dst_name, object_copy_plist,
                    link_creation_plist);
        }
    }

    /**
     * H5Oget_info_by_name returns information about the object. This method follows soft links and
     * returns information about the link target, rather than the link.
     * <p>
     * If not <code>null</code>, <var>info</var> needs to be an array of length 5 and will return
     * the following information in each index:
     * <ul>
     * <li>0: filenumber that the object is in</li>
     * <li>1: address of the object in the file</li>
     * <li>2: reference count of the object (will be {@code > 1} if more than one hard link exists
     * to the object)</li>
     * <li>3: creation time of the object (in seconds since start of the epoch)</li>
     * <li>4: number of attributes that this object has</li>
     * </ul>
     * 
     * @param loc_id File or group identifier within which object is to be open.
     * @param object_name Name of object to get info for.
     * @param infoOrNull If not <code>null</code>, it will return additional information about this
     *            object. Needs to be either <code>null</code> or an array of length 5.
     * @param exception_when_non_existent If <code>true</code>, -1 will be returned when the object
     *            does not exist, otherwise a HDF5LibraryException will be thrown.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Oget_info_by_name(int loc_id, String object_name, long[] infoOrNull,
            boolean exception_when_non_existent) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Oget_info_by_name(loc_id, object_name, infoOrNull,
                    exception_when_non_existent);
        }
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5G: Link Interface Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5Lcreate_hard creates a hard link for an already existing object.
     * 
     * @param obj_loc_id File, group, dataset, or datatype identifier of the existing object
     * @param obj_name A name of the existing object
     * @param link_loc_id Location identifier of the link to create
     * @param link_name Name of the link to create
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - current_name or name is null.
     */
    public static int H5Lcreate_hard(int obj_loc_id, String obj_name, int link_loc_id,
            String link_name, int lcpl_id, int lapl_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5
                    .H5Lcreate_hard(obj_loc_id, obj_name, link_loc_id, link_name, lcpl_id, lapl_id);
        }
    }

    /**
     * H5Lcreate_soft creates a soft link to some target path.
     * 
     * @param target_path The path of the link target
     * @param link_loc_id Location identifier of the link to create
     * @param link_name Name of the link to create
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - current_name or name is null.
     */
    public static int H5Lcreate_soft(String target_path, int link_loc_id, String link_name,
            int lcpl_id, int lapl_id) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lcreate_soft(target_path, link_loc_id, link_name, lcpl_id, lapl_id);
        }
    }

    /**
     * H5Lcreate_external creates an external link to some object in another file.
     * 
     * @param file_name File name of the link target
     * @param obj_name Object name of the link target
     * @param link_loc_id Location identifier of the link to create
     * @param link_name Name of the link to create
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - current_name or name is null.
     */
    public static int H5Lcreate_external(String file_name, String obj_name, int link_loc_id,
            String link_name, int lcpl_id, int lapl_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lcreate_external(file_name, obj_name, link_loc_id, link_name, lcpl_id,
                    lapl_id);
        }
    }

    /**
     * H5Lmove moves a link atomically to a new group or renames it.
     * 
     * @param src_loc_id The old location identifier of the object to be renamed
     * @param src_name The old name of the object to be renamed
     * @param dst_loc_id The new location identifier of the link
     * @param dst_name The new name the object
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - current_name or name is null.
     */
    public static int H5Lmove(int src_loc_id, String src_name, int dst_loc_id, String dst_name,
            int lcpl_id, int lapl_id) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lmove(src_loc_id, src_name, dst_loc_id, dst_name, lcpl_id, lapl_id);
        }
    }

    /**
     * H5Lexists returns <code>true</code> if a link with <var>name</var> exists and <code>false
     * </code> otherwise.
     * <p>
     * <i>Note:</i> The Java wrapper differs from the low-level C routine in that it will return
     * <code>false</code> if <var>name</var> is a path that contains groups which don't exist (the C
     * routine will give you an <code>H5E_NOTFOUND</code> in this case).
     */
    public static boolean H5Lexists(int loc_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lexists(loc_id, name);
        }
    }

    /**
     * H5Lget_link_info returns the type of the link. If <code>lname != null</code> and
     * <var>name</var> is a symbolic link, <code>lname[0]</code> will contain the target of the
     * link. If <var>exception_when_non_existent</var> is <code>true</code>, the method will throw
     * an exception when the link does not exist, otherwise -1 will be returned.
     */
    public static int H5Lget_link_info(int loc_id, String name, String[] lname,
            boolean exception_when_non_existent) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lget_link_info(loc_id, name, lname, exception_when_non_existent);
        }
    }

    /**
     * H5Lget_link_info_all returns the names, types and link targets of all links in group
     * <var>name</var>.
     */
    public static int H5Lget_link_info_all(final int loc_id, final String name,
            final String[] oname, final int[] type, final String[] lname)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lget_link_info_all(loc_id, name, oname, type, lname);
        }
    }

    public static int H5Lget_link_info_all(int loc_id, String name, String[] oname, int[] type,
            String[] lname, int n) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lget_link_info_all(loc_id, name, oname, type, lname, n);
        }
    }

    /**
     * H5Lget_link_names_all returns the names of all links in group <var>name</var>.
     */
    public static int H5Lget_link_names_all(final int loc_id, final String name,
            final String[] oname) throws HDF5LibraryException, NullPointerException
            {
                synchronized (ncsa.hdf.hdf5lib.H5.class)
                {
                    return H5.H5Lget_link_names_all(loc_id, name, oname);
                }
            }

    public static int H5Lget_link_names_all(int loc_id, String name, String[] oname, int n)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Lget_link_names_all(loc_id, name, oname, n);
        }
    }

}
