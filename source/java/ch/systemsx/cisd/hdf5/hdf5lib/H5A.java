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
 * Low-level interface for HDF5 attribute functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 *
 * @author Bernd Rinn
 */
public class H5A
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Lexists returns <code>true</code> if an attribute with <var>name</var> exists for the
     * object defined by <var>obj_id</var> and <code> false </code> otherwise.
     */
    public static boolean H5Aexists(int obj_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aexists(obj_id, name);
        }
    }

    /**
     * H5Acreate creates an attribute which is attached to the object specified with loc_id.
     * 
     * @param loc_id IN: Object (dataset, group, or named datatype) to be attached to.
     * @param name IN: Name of attribute to create.
     * @param type_id IN: Identifier of datatype for attribute.
     * @param space_id IN: Identifier of dataspace for attribute.
     * @param create_plist_id IN: Identifier of creation property list (currently not used).
     * @param access_plist_id IN: Attribute access property list identifier (currently not used).
     * @return an attribute identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Acreate(int loc_id, String name, int type_id, int space_id,
            int create_plist_id, int access_plist_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Acreate(loc_id, name, type_id, space_id, create_plist_id, access_plist_id);
        }
    }

    /**
     * H5Aopen_name opens an attribute specified by its name, name, which is attached to the object
     * specified with loc_id.
     * 
     * @param loc_id IN: Identifier of a group, dataset, or named datatype atttribute
     * @param name IN: Attribute name.
     * @return attribute identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Aopen_name(int loc_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aopen_name(loc_id, name);
        }
    }

    /**
     * H5Aopen_idx opens an attribute which is attached to the object specified with loc_id. The
     * location object may be either a group, dataset, or named datatype, all of which may have any
     * sort of attribute.
     * 
     * @param loc_id IN: Identifier of the group, dataset, or named datatype attribute
     * @param idx IN: Index of the attribute to open.
     * @return attribute identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Aopen_idx(int loc_id, int idx) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aopen_idx(loc_id, idx);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, byte[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, short[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, int[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, long[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, float[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Awrite writes an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is written from buf to the file.
     * 
     * @param attr_id IN: Identifier of an attribute to write.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Data to be written.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data is null.
     */
    public static int H5Awrite(int attr_id, int mem_type_id, double[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Awrite(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5AwriteString writes a (partial) variable length String attribute, specified by its
     * identifier attr_id, from the application memory buffer buf into the file.
     * 
     * @param attr_id Identifier of the dataset read from.
     * @param mem_type_id Identifier of the memory datatype.
     * @param buf Buffer with data to be written to the file.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5AwriteString(int attr_id, int mem_type_id, String[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5AwriteString(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Acopy copies the content of one attribute to another.
     * 
     * @param src_aid the identifier of the source attribute
     * @param dst_aid the identifier of the destinaiton attribute
     */
    public static int H5Acopy(int src_aid, int dst_aid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Acopy(src_aid, dst_aid);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, byte[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, short[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, int[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, long[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, float[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aread reads an attribute, specified with attr_id. The attribute's memory datatype is
     * specified with mem_type_id. The entire attribute is read into buf from the file.
     * 
     * @param attr_id IN: Identifier of an attribute to read.
     * @param mem_type_id IN: Identifier of the attribute datatype (in memory).
     * @param buf IN: Buffer for data to be read.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Aread(int attr_id, int mem_type_id, double[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aread(attr_id, mem_type_id, buf);
        }
    }

    public static int H5AreadVL(int attr_id, int mem_type_id, String[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5AreadVL(attr_id, mem_type_id, buf);
        }
    }

    /**
     * H5Aget_space retrieves a copy of the dataspace for an attribute.
     * 
     * @param attr_id IN: Identifier of an attribute.
     * @return attribute dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Aget_space(int attr_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aget_space(attr_id);
        }
    }

    /**
     * H5Aget_type retrieves a copy of the datatype for an attribute.
     * 
     * @param attr_id IN: Identifier of an attribute.
     * @return a datatype identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Aget_type(int attr_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aget_type(attr_id);
        }
    }

    /**
     * H5Aget_name retrieves the name of an attribute specified by the identifier, attr_id.
     * 
     * @param attr_id IN: Identifier of the attribute.
     * @param buf_size IN: The size of the buffer to store the name in.
     * @param name OUT: Buffer to store name in.
     * @exception ArrayIndexOutOfBoundsException JNI error writing back array
     * @exception ArrayStoreException JNI error writing back array
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     * @exception IllegalArgumentException - bub_size <= 0.
     * @return the length of the attribute's name if successful.
     */
    public static long H5Aget_name(int attr_id, long buf_size, String[] name)
            throws ArrayIndexOutOfBoundsException, ArrayStoreException, HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aget_name(attr_id, buf_size, name);
        }
    }

    /**
     * H5Aget_num_attrs returns the number of attributes attached to the object specified by its
     * identifier, loc_id.
     * 
     * @param loc_id IN: Identifier of a group, dataset, or named datatype.
     * @return the number of attributes if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Aget_num_attrs(int loc_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aget_num_attrs(loc_id);
        }
    }

    /**
     * H5Adelete removes the attribute specified by its name, name, from a dataset, group, or named
     * datatype.
     * 
     * @param loc_id IN: Identifier of the dataset, group, or named datatype.
     * @param name IN: Name of the attribute to delete.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Adelete(int loc_id, String name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Adelete(loc_id, name);
        }
    }

    /**
     * H5Aclose terminates access to the attribute specified by its identifier, attr_id.
     * 
     * @param attr_id IN: Attribute to release access to.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Aclose(int attr_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Aclose(attr_id);
        }
    }

}
