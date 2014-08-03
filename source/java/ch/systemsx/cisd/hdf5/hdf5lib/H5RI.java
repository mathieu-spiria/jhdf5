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
 * Low-level interface for HDF5 reference and identifier functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5RI
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5R: Reference Interface Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5Rcreate creates the reference, ref, of the type specified in ref_type, pointing to the
     * object name located at loc_id.
     * 
     * @param loc_id IN: Location identifier used to locate the object being pointed to.
     * @param name IN: Name of object at location loc_id.
     * @param ref_type IN: Type of reference.
     * @param space_id IN: Dataspace identifier with selection.
     * @return the reference (byte[]) if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - an input array is null.
     * @exception IllegalArgumentException - an input array is invalid.
     */
    public static byte[] H5Rcreate(final int loc_id, final String name, final int ref_type,
            final int space_id) throws HDF5LibraryException, NullPointerException,
            IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rcreate(loc_id, name, ref_type, space_id);
        }
    }

    /**
     * H5Rcreate creates the object references, pointing to the object names located at loc_id.
     * 
     * @param loc_id IN: Location identifier used to locate the object being pointed to.
     * @param name IN: Names of objects at location loc_id.
     * @return the reference (long[]) if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - an input array is null.
     * @exception IllegalArgumentException - an input array is invalid.
     */
    public static long[] H5Rcreate(final int loc_id, final String[] name)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rcreate(loc_id, name);
        }
    }

    /**
     * Given a reference to some object, H5Rdereference opens that object and return an identifier.
     * 
     * @param loc_id IN: Location identifier used to locate the object being pointed to.
     * @param ref IN: reference to an object
     * @return valid identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - output array is null.
     * @exception IllegalArgumentException - output array is invalid.
     */
    public static int H5Rdereference(int loc_id, long ref)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rdereference(loc_id, ref);
        }
    }

    /**
     * Given a reference to some object, H5Rdereference opens that object and return an identifier.
     * 
     * @param loc_id IN: Location identifier used to locate the object being pointed to.
     * @param ref_type IN: The reference type of ref.
     * @param ref IN: reference to an object
     * @return valid identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - output array is null.
     * @exception IllegalArgumentException - output array is invalid.
     */
    public static int H5Rdereference(int loc_id, int ref_type, byte[] ref)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rdereference(loc_id, ref_type, ref);
        }
    }

    /**
     * Given a reference to an object ref, H5Rget_region creates a copy of the dataspace of the
     * dataset pointed to and defines a selection in the copy which is the region pointed to.
     * 
     * @param loc_id IN: loc_id of the reference object.
     * @param ref_type IN: The reference type of ref.
     * @param ref OUT: the reference to the object and region
     * @return a valid identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - output array is null.
     * @exception IllegalArgumentException - output array is invalid.
     */
    public static int H5Rget_region(int loc_id, int ref_type, byte[] ref)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rget_region(loc_id, ref_type, ref);
        }
    }

    /**
     * Given a reference to an object, H5Rget_obj_type returns the type of the object pointed to.
     * 
     * @param loc_id Identifier of the reference object.
     * @param ref_type Type of reference to query.
     * @param ref The reference.
     * @return a valid identifier if successful; otherwise a negative value is returned to signal
     *         failure.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - array is null.
     * @exception IllegalArgumentException - array is invalid.
     */
    public static int H5Rget_obj_type(int loc_id, int ref_type, byte[] ref)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rget_obj_type(loc_id, ref_type, ref);
        }
    }

    /**
     * Given a reference to an object, H5Rget_name returns the name (path) of the object pointed to.
     * 
     * @param loc_id Identifier of the reference object.
     * @param ref_type Type of reference to query.
     * @param ref The reference.
     * @return The path of the object being pointed to, or an empty string, if the object being
     *         pointed to has no name.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - array is null.
     * @exception IllegalArgumentException - array is invalid.
     */
    public static String H5Rget_name(int loc_id, int ref_type, byte[] ref)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rget_name(loc_id, ref_type, ref);
        }
    }

    /**
     * Given a reference to an object, H5Rget_name returns the name (path) of the object pointed to.
     * 
     * @param loc_id Identifier of the reference object.
     * @param ref The reference.
     * @return The path of the object being pointed to, or an empty string, if the object being
     *         pointed to has no name.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - array is null.
     * @exception IllegalArgumentException - array is invalid.
     */
    public static String H5Rget_name(int loc_id, long ref)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rget_name(loc_id, ref);
        }
    }

    /**
     * Given an array of object references (ref), H5Rget_name returns the names (paths) of the
     * objects pointed to.
     * 
     * @param loc_id Identifier of the reference object.
     * @param ref The references.
     * @return The paths of the objects being pointed to, or an empty string, if an object being
     *         pointed to has no name.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - array is null.
     * @exception IllegalArgumentException - array is invalid.
     */
    public static String[] H5Rget_name(int loc_id, long[] ref)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Rget_name(loc_id, ref);
        }
    }

    // ////////////////////////////////////////////////////////////
    // //
    // H5I: Identifier Interface Functions //
    // //
    // ////////////////////////////////////////////////////////////

    /**
     * H5Iget_type retrieves the type of the object identified by obj_id.
     * 
     * @param obj_id IN: Object identifier whose type is to be determined.
     * @return the object type if successful; otherwise H5I_BADID.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Iget_type(int obj_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Iget_type(obj_id);
        }
    }

    public static long H5Iget_name(int obj_id, String[] name, long size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Iget_name(obj_id, name, size);
        }
    }

    public static int H5Iget_ref(int obj_id) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Iget_ref(obj_id);
        }
    }

    public static int H5Iinc_ref(int obj_id) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Iinc_ref(obj_id);
        }
    }

    public static int H5Idec_ref(int obj_id) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Idec_ref(obj_id);
        }
    }

    public static int H5Iget_file_id(int obj_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Iget_file_id(obj_id);
        }
    }

}
