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

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Low-level interface for HDF5 property list functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5P
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Pcreate creates a new property as an instance of some property list class.
     * 
     * @param type IN: The type of property list to create.
     * @return a property list identifier (plist) if successful; otherwise Fail (-1).
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pcreate(int type) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcreate(type);
        }
    }

    /**
     * H5Pclose terminates access to a property list.
     * 
     * @param plist IN: Identifier of the property list to terminate access to.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pclose(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pclose(plist);
        }
    }

    /**
     * H5Pget_class returns the property list class for the property list identified by the plist
     * parameter.
     * 
     * @param plist IN: Identifier of property list to query.
     * @return a property list class if successful. Otherwise returns H5P_NO_CLASS (-1).
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pget_class(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_class(plist);
        }
    }

    /**
     * H5Pcopy copies an existing property list to create a new property list.
     * 
     * @param plist IN: Identifier of property list to duplicate.
     * @return a property list identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pcopy(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcopy(plist);
        }
    }

    /**
     * H5Pget_version retrieves the version information of various objects for a file creation
     * property list.
     * 
     * @param plist IN: Identifier of the file creation property list.
     * @param version_info OUT: version information.
     * 
     *            <pre>
     * 
     *            version_info[0] = boot // boot block version number version_info[1] = freelist //
     *            global freelist version version_info[2] = stab // symbol tabl version number
     *            version_info[3] = shhdr // hared object header version
     * 
     * </pre>
     * @return a non-negative value, with the values of version_info initialized, if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - version_info is null.
     * @exception IllegalArgumentException - version_info is illegal.
     */
    public static int H5Pget_version(int plist, int[] version_info) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_version(plist, version_info);
        }
    }

    /**
     * H5Pset_userblock sets the user block size of a file creation property list.
     * 
     * @param plist IN: Identifier of property list to modify.
     * @param size IN: Size of the user-block in bytes.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_userblock(int plist, long size) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_userblock(plist, size);
        }
    }

    /**
     * H5Pget_userblock retrieves the size of a user block in a file creation property list.
     * 
     * @param plist IN: Identifier for property list to query.
     * @param size OUT: Pointer to location to return user-block size.
     * @return a non-negative value and the size of the user block; if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size is null.
     */
    public static int H5Pget_userblock(int plist, long[] size) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_userblock(plist, size);
        }
    }

    /**
     * H5Pset_small_data_block_size reserves blocks of size bytes for the contiguous storage of the
     * raw data portion of small datasets.
     * 
     * @param plist IN: Identifier of property list to modify.
     * @param size IN: Size of the blocks in bytes.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_small_data_block_size(int plist, long size)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_small_data_block_size(plist, size);
        }
    }

    /**
     * H5Pget_small_data_block_size retrieves the size of a block of small data in a file creation
     * property list.
     * 
     * @param plist IN: Identifier for property list to query.
     * @param size OUT: Pointer to location to return block size.
     * @return a non-negative value and the size of the user block; if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size is null.
     */
    public static int H5Pget_small_data_block_size(int plist, long[] size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_small_data_block_size(plist, size);
        }
    }

    /**
     * H5Pset_sizes sets the byte size of the offsets and lengths used to address objects in an HDF5
     * file.
     * 
     * @param plist IN: Identifier of property list to modify.
     * @param sizeof_addr IN: Size of an object offset in bytes.
     * @param sizeof_size IN: Size of an object length in bytes.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_sizes(int plist, int sizeof_addr, int sizeof_size)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_sizes(plist, sizeof_addr, sizeof_size);
        }
    }

    /**
     * H5Pget_sizes retrieves the size of the offsets and lengths used in an HDF5 file. This
     * function is only valid for file creation property lists.
     * 
     * @param plist IN: Identifier of property list to query.
     * @param size OUT: the size of the offsets and length.
     * 
     *            <pre>
     * 
     *            size[0] = sizeof_addr // offset size in bytes size[1] = sizeof_size // length size
     *            in bytes
     * 
     * </pre>
     * @return a non-negative value with the sizes initialized; if successful;
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size is null.
     * @exception IllegalArgumentException - size is invalid.
     */
    public static int H5Pget_sizes(int plist, int[] size) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_sizes(plist, size);
        }
    }

    /**
     * H5Pset_sym_k sets the size of parameters used to control the symbol table nodes.
     * 
     * @param plist IN: Identifier for property list to query.
     * @param ik IN: Symbol table tree rank.
     * @param lk IN: Symbol table node size.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_sym_k(int plist, int ik, int lk) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_sym_k(plist, ik, lk);
        }
    }

    /**
     * H5Pget_sym_k retrieves the size of the symbol table B-tree 1/2 rank and the symbol table leaf
     * node 1/2 size.
     * 
     * @param plist IN: Property list to query.
     * @param size OUT: the symbol table's B-tree 1/2 rank and leaf node 1/2 size.
     * 
     *            <pre>
     * 
     *            size[0] = ik // the symbol table's B-tree 1/2 rank size[1] = lk // leaf node 1/2
     *            size
     * 
     * </pre>
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size is null.
     * @exception IllegalArgumentException - size is invalid.
     */
    public static int H5Pget_sym_k(int plist, int[] size) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_sym_k(plist, size);
        }
    }

    /**
     * H5Pset_istore_k sets the size of the parameter used to control the B-trees for indexing
     * chunked datasets.
     * 
     * @param plist IN: Identifier of property list to query.
     * @param ik IN: 1/2 rank of chunked storage B-tree.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_istore_k(int plist, int ik) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_istore_k(plist, ik);
        }
    }

    /**
     * H5Pget_istore_k queries the 1/2 rank of an indexed storage B-tree.
     * 
     * @param plist IN: Identifier of property list to query.
     * @param ik OUT: Pointer to location to return the chunked storage B-tree 1/2 rank.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - ik array is null.
     */
    public static int H5Pget_istore_k(int plist, int[] ik) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_istore_k(plist, ik);
        }
    }

    /**
     * H5Pset_layout sets the type of storage used store the raw data for a dataset.
     * 
     * @param plist IN: Identifier of property list to query.
     * @param layout IN: Type of storage layout for raw data.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_layout(int plist, int layout) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_layout(plist, layout);
        }
    }

    /**
     * H5Pget_layout returns the layout of the raw data for a dataset.
     * 
     * @param plist IN: Identifier for property list to query.
     * @return the layout type of a dataset creation property list if successful. Otherwise returns
     *         H5D_LAYOUT_ERROR (-1).
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pget_layout(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_layout(plist);
        }
    }

    /**
     * H5Pset_chunk sets the size of the chunks used to store a chunked layout dataset.
     * 
     * @param plist IN: Identifier for property list to query.
     * @param ndims IN: The number of dimensions of each chunk.
     * @param dim IN: An array containing the size of each chunk.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - dims array is null.
     * @exception IllegalArgumentException - dims <=0
     */
    public static int H5Pset_chunk(int plist, int ndims, byte[] dim) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_chunk(plist, ndims, dim);
        }
    }

    public static int H5Pset_chunk(final int plist, final int ndims, final long[] dim)
            throws HDF5Exception, NullPointerException, IllegalArgumentException
    {
        if (dim == null)
        {
            return -1;
        }

        final byte[] thedims = HDFNativeData.longToByte(dim);

        return H5Pset_chunk(plist, ndims, thedims);
    }

    /**
     * H5Pget_chunk retrieves the size of chunks for the raw data of a chunked layout dataset.
     * 
     * @param plist IN: Identifier of property list to query.
     * @param max_ndims IN: Size of the dims array.
     * @param dims OUT: Array to store the chunk dimensions.
     * @return chunk dimensionality successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - dims array is null.
     * @exception IllegalArgumentException - max_ndims <=0
     */
    public static int H5Pget_chunk(int plist, int max_ndims, long[] dims)
            throws HDF5LibraryException, NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_chunk(plist, max_ndims, dims);
        }
    }

    /**
     * H5Pset_alignment sets the alignment properties of a file access property list so that any
     * file object >= THRESHOLD bytes will be aligned on an address which is a multiple of
     * ALIGNMENT.
     * 
     * @param plist IN: Identifier for a file access property list.
     * @param threshold IN: Threshold value.
     * @param alignment IN: Alignment value.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_alignment(int plist, long threshold, long alignment)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_alignment(plist, threshold, alignment);
        }
    }

    /**
     * H5Pget_alignment retrieves the current settings for alignment properties from a file access
     * property list.
     * 
     * @param plist IN: Identifier of a file access property list.
     * @param alignment OUT: threshold value and alignment value.
     * 
     *            <pre>
     * 
     *            alignment[0] = threshold // threshold value alignment[1] = alignment // alignment
     *            value
     * 
     * </pre>
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - aligment array is null.
     * @exception IllegalArgumentException - aligment array is invalid.
     */
    public static int H5Pget_alignment(int plist, long[] alignment) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_alignment(plist, alignment);
        }
    }

    /**
     * H5Pset_external adds an external file to the list of external files.
     * 
     * @param plist IN: Identifier of a dataset creation property list.
     * @param name IN: Name of an external file.
     * @param offset IN: Offset, in bytes, from the beginning of the file to the location in the
     *            file where the data starts.
     * @param size IN: Number of bytes reserved in the file for the data.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Pset_external(int plist, String name, long offset, long size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_external(plist, name, offset, size);
        }
    }

    /**
     * H5Pget_external_count returns the number of external files for the specified dataset.
     * 
     * @param plist IN: Identifier of a dataset creation property list.
     * @return the number of external files if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pget_external_count(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_external_count(plist);
        }
    }

    /**
     * H5Pget_external returns information about an external file.
     * 
     * @param plist IN: Identifier of a dataset creation property list.
     * @param idx IN: External file index.
     * @param name_size IN: Maximum length of name array.
     * @param name OUT: Name of the external file.
     * @param size OUT: the offset value and the size of the external file data.
     * 
     *            <pre>
     * 
     *            size[0] = offset // a location to return an offset value size[1] = size // a
     *            location to return the size of // the external file data.
     * 
     * </pre>
     * @return a non-negative value if successful
     * @exception ArrayIndexOutOfBoundsException Fatal error on Copyback
     * @exception ArrayStoreException Fatal error on Copyback
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name or size is null.
     * @exception IllegalArgumentException - name_size <= 0 .
     */
    public static int H5Pget_external(int plist, int idx, int name_size, String[] name, long[] size)
            throws ArrayIndexOutOfBoundsException, ArrayStoreException, HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_external(plist, idx, name_size, name, size);
        }
    }

    /**
     * H5Pset_fill_value sets the fill value for a dataset creation property list.
     * 
     * @param plist_id IN: Property list identifier.
     * @param type_id IN: The datatype identifier of value.
     * @param value IN: The fill value.
     * @return a non-negative value if successful
     * @exception HDF5Exception - Error converting data array
     */
    public static int H5Pset_fill_value(int plist_id, int type_id, byte[] value)
            throws HDF5Exception
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fill_value(plist_id, type_id, value);
        }
    }

    /**
     * H5Pget_fill_value queries the fill value property of a dataset creation property list. <b>NOT
     * IMPLEMENTED YET</B>
     * 
     * @param plist_id IN: Property list identifier.
     * @param type_id IN: The datatype identifier of value.
     * @param value IN: The fill value.
     * @return a non-negative value if successful
     */
    public static int H5Pget_fill_value(int plist_id, int type_id, byte[] value)
            throws HDF5Exception
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_fill_value(plist_id, type_id, value);
        }
    }

    /**
     * H5Pset_filter adds the specified filter and corresponding properties to the end of an output
     * filter pipeline.
     * 
     * @param plist IN: Property list identifier.
     * @param filter IN: Filter to be added to the pipeline.
     * @param flags IN: Bit vector specifying certain general properties of the filter.
     * @param cd_nelmts IN: Number of elements in cd_values
     * @param cd_values IN: Auxiliary data for the filter.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_filter(int plist, int filter, int flags, int cd_nelmts, int[] cd_values)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_filter(plist, filter, flags, cd_nelmts, cd_values);
        }
    }

    /**
     * H5Pget_nfilters returns the number of filters defined in the filter pipeline associated with
     * the property list plist.
     * 
     * @param plist IN: Property list identifier.
     * @return the number of filters in the pipeline if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pget_nfilters(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_nfilters(plist);
        }
    }

    /**
     * H5Pget_filter returns information about a filter, specified by its filter number, in a filter
     * pipeline, specified by the property list with which it is associated.
     * 
     * @param plist IN: Property list identifier.
     * @param filter_number IN: Sequence number within the filter pipeline of the filter for which
     *            information is sought.
     * @param flags OUT: Bit vector specifying certain general properties of the filter.
     * @param cd_nelmts IN/OUT: Number of elements in cd_values
     * @param cd_values OUT: Auxiliary data for the filter.
     * @param namelen IN: Anticipated number of characters in name.
     * @param name OUT: Name of the filter.
     * @return the filter identification number if successful. Otherwise returns H5Z_FILTER_ERROR
     *         (-1).
     * @exception ArrayIndexOutOfBoundsException Fatal error on Copyback
     * @exception ArrayStoreException Fatal error on Copyback
     * @exception NullPointerException - name or an array is null.
     */
    public static int H5Pget_filter(int plist, int filter_number, int[] flags, int[] cd_nelmts,
            int[] cd_values, int namelen, String[] name) throws ArrayIndexOutOfBoundsException,
            ArrayStoreException, HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_filter(plist, filter_number, flags, cd_nelmts, cd_values, namelen,
                    name);
        }
    }

    /**
     * H5Pset_cache sets the number of elements (objects) in the meta data cache and the total
     * number of bytes in the raw data chunk cache.
     * 
     * @param plist IN: Identifier of the file access property list.
     * @param mdc_nelmts IN: Number of elements (objects) in the meta data cache.
     * @param rdcc_nbytes IN: Total size of the raw data chunk cache, in bytes.
     * @param rdcc_w0 IN: Preemption policy.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_cache(int plist, int mdc_nelmts, int rdcc_nelmts, int rdcc_nbytes,
            double rdcc_w0) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_cache(plist, mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0);
        }
    }

    /**
     * Retrieves the maximum possible number of elements in the meta data cache and the maximum
     * possible number of bytes and the RDCC_W0 value in the raw data chunk cache.
     * 
     * @param plist IN: Identifier of the file access property list.
     * @param mdc_nelmts IN/OUT: Number of elements (objects) in the meta data cache.
     * @param rdcc_nbytes IN/OUT: Total size of the raw data chunk cache, in bytes.
     * @param rdcc_w0 IN/OUT: Preemption policy.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - an array is null.
     */
    public static int H5Pget_cache(int plist, int[] mdc_nelmts, int[] rdcc_nelmts,
            int[] rdcc_nbytes, double[] rdcc_w0) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_cache(plist, mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0);
        }
    }

    /**
     * H5Pset_buffer sets type conversion and background buffers. status to TRUE or FALSE. Given a
     * dataset transfer property list, H5Pset_buffer sets the maximum size for the type conversion
     * buffer and background buffer and optionally supplies pointers to application-allocated
     * buffers. If the buffer size is smaller than the entire amount of data being transferred
     * between the application and the file, and a type conversion buffer or background buffer is
     * required, then strip mining will be used. Note that there are minimum size requirements for
     * the buffer. Strip mining can only break the data up along the first dimension, so the buffer
     * must be large enough to accommodate a complete slice that encompasses all of the remaining
     * dimensions. For example, when strip mining a 100x200x300 hyperslab of a simple data space,
     * the buffer must be large enough to hold 1x200x300 data elements. When strip mining a
     * 100x200x300x150 hyperslab of a simple data space, the buffer must be large enough to hold
     * 1x200x300x150 data elements. If tconv and/or bkg are null pointers, then buffers will be
     * allocated and freed during the data transfer.
     * 
     * @param plist Identifier for the dataset transfer property list.
     * @param size Size, in bytes, of the type conversion and background buffers.
     * @param tconv byte array of application-allocated type conversion buffer.
     * @param bkg byte array of application-allocated background buffer.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception IllegalArgumentException - plist is invalid.
     */
    public static int H5Pset_buffer(int plist, int size, byte[] tconv, byte[] bkg)
            throws HDF5LibraryException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_buffer(plist, size, tconv, bkg);
        }
    }

    /**
     * HH5Pget_buffer gets type conversion and background buffers. Returns buffer size, in bytes, if
     * successful; otherwise 0 on failure.
     * 
     * @param plist Identifier for the dataset transfer property list.
     * @param tconv byte array of application-allocated type conversion buffer.
     * @param bkg byte array of application-allocated background buffer.
     * @return buffer size, in bytes, if successful; otherwise 0 on failure
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception IllegalArgumentException - plist is invalid.
     */
    public static int H5Pget_buffer(int plist, byte[] tconv, byte[] bkg)
            throws HDF5LibraryException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_buffer(plist, tconv, bkg);
        }
    }

    /**
     * H5Pset_preserve sets the dataset transfer property list status to TRUE or FALSE.
     * 
     * @param plist IN: Identifier for the dataset transfer property list.
     * @param status IN: Status of for the dataset transfer property list.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception IllegalArgumentException - plist is invalid.
     */
    public static int H5Pset_preserve(int plist, boolean status) throws HDF5LibraryException,
            IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_preserve(plist, status);
        }
    }

    /**
     * H5Pget_preserve checks the status of the dataset transfer property list.
     * 
     * @param plist IN: Identifier for the dataset transfer property list.
     * @return TRUE or FALSE if successful; otherwise returns a negative value
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pget_preserve(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_preserve(plist);
        }
    }

    /**
     * H5Pset_deflate sets the compression method for a dataset.
     * 
     * @param plist IN: Identifier for the dataset creation property list.
     * @param level IN: Compression level.
     * @return non-negative if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_deflate(int plist, int level) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_deflate(plist, level);
        }
    }

    /**
     * H5Pset_nbit sets the compression method for a dataset to n-bits.
     * <p>
     * Keeps only n-bits from an integer or float value.
     * 
     * @param plist IN: Identifier for the dataset creation property list.
     * @return non-negative if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_nbit(int plist) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_nbit(plist);
        }
    }

    /**
     * H5Pset_scaleoffset sets the compression method for a dataset to scale_offset.
     * <p>
     * Generally speaking, Scale-Offset compression performs a scale and/or offset operation on each
     * data value and truncates the resulting value to a minimum number of bits (MinBits) before
     * storing it. The current Scale-Offset filter supports integer and floating-point datatype.
     * 
     * @param plist IN: Identifier for the dataset creation property list.
     * @param scale_type IN: One of {@link HDF5Constants#H5Z_SO_INT},
     *            {@link HDF5Constants#H5Z_SO_FLOAT_DSCALE} or
     *            {@link HDF5Constants#H5Z_SO_FLOAT_ESCALE}. Note that
     *            {@link HDF5Constants#H5Z_SO_FLOAT_ESCALE} is not implemented as of HDF5 1.8.2.
     * @return non-negative if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Pset_scaleoffset(int plist, int scale_type, int scale_factor)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_scaleoffset(plist, scale_type, scale_factor);
        }
    }

    /**
     * H5Pset_create_intermediate_group pecifies in property list whether to create missing
     * intermediate groups.
     * <p>
     * H5Pset_create_intermediate_group specifies whether to set the link creation property list
     * lcpl_id so that calls to functions that create objects in groups different from the current
     * working group will create intermediate groups that may be missing in the path of a new or
     * moved object.
     * <p>
     * Functions that create objects in or move objects to a group other than the current working
     * group make use of this property. H5Gcreate_anon and H5Lmove are examles of such functions.
     * <p>
     * If crt_intermed_group is <code>true</code>, the H5G_CRT_INTMD_GROUP will be added to lcpl_id
     * (if it is not already there). Missing intermediate groups will be created upon calls to
     * functions such as those listed above that use lcpl_id.
     * <p>
     * If crt_intermed_group is <code>false</code>, the H5G_CRT_INTMD_GROUP, if present, will be
     * removed from lcpl_id. Missing intermediate groups will not be created upon calls to functions
     * such as those listed above that use lcpl_id.
     * 
     * @param lcpl_id Link creation property list identifier
     * @param crt_intermed_group Flag specifying whether to create intermediate groups upon the
     *            creation of an object
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static void H5Pset_create_intermediate_group(int lcpl_id, boolean crt_intermed_group)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            H5.H5Pset_create_intermediate_group(lcpl_id, crt_intermed_group);
        }
    }

    /**
     * Determines whether property is set to enable creating missing intermediate groups.
     * 
     * @return <code>true</code> if intermediate groups are created, <code>false</code> otherwise.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static boolean H5Pget_create_intermediate_group(int lcpl_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_create_intermediate_group(lcpl_id);
        }
    }

    /**
     * Returns a dataset transfer property list (<code>H5P_DATASET_XFER</code>) that has a
     * conversion exception handler set which abort conversions that triggers overflows.
     */
    public static int H5Pcreate_xfer_abort_overflow()
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcreate_xfer_abort_overflow();
        }
    }

    /**
     * Returns a dataset transfer property list (<code>H5P_DATASET_XFER</code>) that has a
     * conversion exception handler set which aborts all conversions.
     */
    public static int H5Pcreate_xfer_abort()
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcreate_xfer_abort();
        }
    }

    public static int H5Pset_alloc_time(int plist_id, int alloc_time) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_alloc_time(plist_id, alloc_time);
        }
    }

    public static int H5Pget_alloc_time(int plist_id, int[] alloc_time)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_alloc_time(plist_id, alloc_time);
        }
    }

    public static int H5Pset_fill_time(int plist_id, int fill_time) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fill_time(plist_id, fill_time);
        }
    }

    public static int H5Pget_fill_time(int plist_id, int[] fill_time) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_fill_time(plist_id, fill_time);
        }
    }

    public static int H5Pfill_value_defined(int plist_id, int[] status)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pfill_value_defined(plist_id, status);
        }
    }

    public static int H5Pset_fletcher32(int plist) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fletcher32(plist);
        }
    }

    public static int H5Pset_edc_check(int plist, int check) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_edc_check(plist, check);
        }
    }

    public static int H5Pget_edc_check(int plist) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_edc_check(plist);
        }
    }

    public static int H5Pset_shuffle(int plist_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_shuffle(plist_id);
        }
    }

    public static int H5Pmodify_filter(int plist, int filter, int flags, long cd_nelmts,
            int[] cd_values) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pmodify_filter(plist, filter, flags, cd_nelmts, cd_values);
        }
    }

    public static int H5Pget_filter_by_id(int plist_id, int filter, int[] flags, long[] cd_nelmts,
            int[] cd_values, long namelen, String[] name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_filter_by_id(plist_id, filter, flags, cd_nelmts, cd_values, namelen,
                    name);
        }
    }

    public static boolean H5Pall_filters_avail(int dcpl_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pall_filters_avail(dcpl_id);
        }
    }

    public static int H5Pset_hyper_vector_size(int dxpl_id, long vector_size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_hyper_vector_size(dxpl_id, vector_size);
        }
    }

    public static int H5Pget_hyper_vector_size(int dxpl_id, long[] vector_size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_hyper_vector_size(dxpl_id, vector_size);
        }
    }

    public static int H5Pset_fclose_degree(int plist, int degree) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fclose_degree(plist, degree);
        }
    }

    public static int H5Pget_fclose_degree(int plist_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_fclose_degree(plist_id);
        }
    }

    public static int H5Pset_fapl_family(int fapl_id, long memb_size, int memb_fapl_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fapl_family(fapl_id, memb_size, memb_fapl_id);
        }
    }

    public static int H5Pget_fapl_family(int fapl_id, long[] memb_size, int[] memb_fapl_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_fapl_family(fapl_id, memb_size, memb_fapl_id);
        }
    }

    public static int H5Pset_fapl_core(int fapl_id, int increment, boolean backing_store)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fapl_core(fapl_id, increment, backing_store);
        }
    }

    public static int H5Pget_fapl_core(int fapl_id, int[] increment, boolean[] backing_store)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_fapl_core(fapl_id, increment, backing_store);
        }
    }

    public static int H5Pset_family_offset(int fapl_id, long offset) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_family_offset(fapl_id, offset);
        }
    }

    public static long H5Pget_family_offset(int fapl_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_family_offset(fapl_id);
        }
    }

    public static int H5Pset_fapl_log(int fapl_id, String logfile, int flags, int buf_size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_fapl_log(fapl_id, logfile, flags, buf_size);
        }
    }

    public static int H5Premove_filter(int obj_id, int filter) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Premove_filter(obj_id, filter);
        }
    }

    /**
     * Creates a new property list class of a given class
     * 
     * @param cls IN: Class of property list to create
     * @return a valid property list identifier if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pcreate_list(int cls) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcreate_list(cls);
        }
    }

    /**
     * Sets a property list value (support integer only)
     * 
     * @param plid IN: Property list identifier to modify
     * @param name IN: Name of property to modify
     * @param value IN: value to set the property to
     * @return a non-negative value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pset(int plid, String name, int value) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset(plid, name, value);
        }
    }

    /**
     * H5Pexist determines whether a property exists within a property list or class
     * 
     * @param plid IN: Identifier for the property to query
     * @param name IN: Name of property to check for
     * @return a positive value if the property exists in the property object; zero if the property
     *         does not exist; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pexist(int plid, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pexist(plid, name);
        }
    }

    /**
     * H5Pget_size retrieves the size of a property's value in bytes
     * 
     * @param plid IN: Identifier of property object to query
     * @param name IN: Name of property to query
     * @return size of a property's value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static long H5Pget_size(int plid, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_size(plid, name);
        }
    }

    /**
     * H5Pget_nprops retrieves the number of properties in a property list or class
     * 
     * @param plid IN: Identifier of property object to query
     * @return number of properties if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static long H5Pget_nprops(int plid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_nprops(plid);
        }
    }

    /**
     * H5Pget_class_name retrieves the name of a generic property list class
     * 
     * @param plid IN: Identifier of property object to query
     * @return name of a property list if successful; null if failed
     * @throws HDF5LibraryException
     */
    public static String H5Pget_class_name(int plid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_class_name(plid);
        }
    }

    /**
     * H5Pget_class_parent retrieves an identifier for the parent class of a property class
     * 
     * @param plid IN: Identifier of the property class to query
     * @return a valid parent class object identifier if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pget_class_parent(int plid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_class_parent(plid);
        }
    }

    /**
     * H5Pisa_class checks to determine whether a property list is a member of the specified class
     * 
     * @param plist IN: Identifier of the property list
     * @param pclass IN: Identifier of the property class
     * @return a positive value if equal; zero if unequal; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pisa_class(int plist, int pclass) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pisa_class(plist, pclass);
        }
    }

    /**
     * H5Pget retrieves a copy of the value for a property in a property list (support integer only)
     * 
     * @param plid IN: Identifier of property object to query
     * @param name IN: Name of property to query
     * @return value for a property if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pget(int plid, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget(plid, name);
        }
    }

    /**
     * H5Pequal determines if two property lists or classes are equal
     * 
     * @param plid1 IN: First property object to be compared
     * @param plid2 IN: Second property object to be compared
     * @return positive value if equal; zero if unequal, a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pequal(int plid1, int plid2) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pequal(plid1, plid2);
        }
    }

    /**
     * H5Pcopy_prop copies a property from one property list or class to another
     * 
     * @param dst_id IN: Identifier of the destination property list or class
     * @param src_id IN: Identifier of the source property list or class
     * @param name IN: Name of the property to copy
     * @return a non-negative value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pcopy_prop(int dst_id, int src_id, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pcopy_prop(dst_id, src_id, name);
        }
    }

    /**
     * H5Premove removes a property from a property list
     * 
     * @param plid IN: Identifier of the property list to modify
     * @param name IN: Name of property to remove
     * @return a non-negative value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Premove(int plid, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Premove(plid, name);
        }
    }

    /**
     * H5Punregister removes a property from a property list class
     * 
     * @param plid IN: Property list class from which to remove permanent property
     * @param name IN: Name of property to remove
     * @return a non-negative value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Punregister(int plid, String name) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Punregister(plid, name);
        }
    }

    /**
     * Closes an existing property list class
     * 
     * @param plid IN: Property list class to close
     * @return a non-negative value if successful; a negative value if failed
     * @throws HDF5LibraryException
     */
    public static int H5Pclose_class(int plid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pclose_class(plid);
        }
    }

    /**
     * Sets the permissible bounds of the library's file format versions.
     * <p>
     * Can be set on the file access property list.
     * <p>
     * As of 1.8.0, only the combinations <code>low=H5F_LIBVER_EARLIEST</code> / <code>
     * high=H5F_LIBVER_LATEST</code> (which is the default and means that 1.6 compatible files are
     * created if no features are used that require a 1.8 format) and <code>low=H5F_LIBVER_LATEST
     * </code> / <code>high=H5F_LIBVER_LATEST</code> (which means that always 1.8 files are created
     * which cannot be read by an earlier library) are allowed.
     * 
     * @param plist_id Property list identifier.
     * @param low The lower permissible bound. One of <code>H5F_LIBVER_LATEST</code> or <code>
     *            H5F_LIBVER_LATEST</code> .
     * @param high The higher permissible bound. Must be <code>H5F_LIBVER_LATEST</code>.
     * @return a non-negative value if successful
     */
    public static int H5Pset_libver_bounds(int plist_id, int low, int high)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_libver_bounds(plist_id, low, high);
        }
    }

    /**
     * Returns the permissible bounds of the library's file format versions.
     * 
     * @param plist_id Property list identifier.
     * @return an array containing <code>[low, high]</code> on success
     */
    public static int[] H5Pget_libver_bounds(int plist_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_libver_bounds(plist_id);
        }
    }

    /**
     * Sets the local heap size hint for an old-style group. This is the chunk size allocated on the
     * heap for a group.
     * 
     * @param gcpl_id The group creation property list to change the heap size hint for
     * @param size_hint The size hint to set.
     * @return a non-negative value if successful
     */
    public static int H5Pset_local_heap_size_hint(int gcpl_id, int size_hint)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_local_heap_size_hint(gcpl_id, size_hint);
        }
    }

    /**
     * Returns the local heap size hint for an old-style group. This is the chunk size allocated on
     * the heap for a group.
     * 
     * @param gcpl_id The group creation property list to change the heap size hint for
     * @return The size hint of the group if successful
     */
    public static int H5Pget_local_heap_size_hint(int gcpl_id)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_local_heap_size_hint(gcpl_id);
        }
    }

    /**
     * Sets the phase change parameters for a new-style group.
     * 
     * @param gcpl_id The group creation property list to set the link phase changes for
     * @param max_compact The maximum number of links in a group to store as header messages
     * @param min_dense The minimum number of links in a group to in the dense format
     * @return a non-negative value if successful
     */
    public static int H5Pset_link_phase_change(int gcpl_id, int max_compact, int min_dense)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_link_phase_change(gcpl_id, max_compact, min_dense);
        }
    }

    /**
     * Returns the phase change parameters for a new-style group.
     * 
     * @param gcpl_id The group creation property list to set the link phase changes for
     * @return the phase change parameters as array [max_compact, min_dense] if successful
     */
    public static int[] H5Pget_link_phase_change(int gcpl_id)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_link_phase_change(gcpl_id);
        }
    }

    /**
     * Sets the character encoding for the given creation property list to the given encoding.
     * 
     * @param cpl_id The creation property list to set the character encoding for.
     * @param encoding The encoding (one of {@link HDF5Constants#H5T_CSET_ASCII} or
     *            {@link HDF5Constants#H5T_CSET_UTF8}) to use.
     * @return a non-negative value if successful
     */
    public static int H5Pset_char_encoding(int cpl_id, int encoding)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pset_char_encoding(cpl_id, encoding);
        }
    }

    /**
     * Returns the character encoding currently set for a creation property list.
     * 
     * @param cpl_id The creation property list to get the character encoding for.
     * @return The encoding, one of {@link HDF5Constants#H5T_CSET_ASCII} or
     *         {@link HDF5Constants#H5T_CSET_UTF8}.
     */
    public static int H5Pget_char_encoding(int cpl_id)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Pget_char_encoding(cpl_id);
        }
    }

}
