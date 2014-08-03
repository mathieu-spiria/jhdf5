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
 * Low-level interface for HDF5 dataspace functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 *
 * @author Bernd Rinn
 */
public class H5S
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Screate creates a new dataspace of a particular type.
     * 
     * @param type The type of dataspace to be created.
     * @return a dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Screate(int type) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Screate(type);
        }
    }

    /**
     * H5Screate_simple creates a new simple data space and opens it for access.
     * 
     * @param rank Number of dimensions of dataspace.
     * @param dims An array of the size of each dimension.
     * @param maxdims An array of the maximum size of each dimension.
     * @return a dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - dims or maxdims is null.
     */
    public static int H5Screate_simple(int rank, byte[] dims, byte[] maxdims)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Screate_simple(rank, dims, maxdims);
        }
    }

    public static int H5Screate_simple(final int rank, final long[] dims, final long[] maxdims)
            throws HDF5Exception, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Screate_simple(rank, dims, maxdims);
        }
    }

    /**
     * H5Scopy creates a new dataspace which is an exact copy of the dataspace identified by
     * space_id.
     * 
     * @param space_id Identifier of dataspace to copy.
     * @return a dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Scopy(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Scopy(space_id);
        }
    }

    /**
     * H5Sselect_elements selects array elements to be included in the selection for the space_id
     * dataspace.
     * 
     * @param space_id Identifier of the dataspace.
     * @param op operator specifying how the new selection is combined.
     * @param num_elements Number of elements to be selected.
     * @param coord A 2-dimensional array specifying the coordinates of the elements.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sselect_elements(int space_id, int op, int num_elements, byte[] coord)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_elements(space_id, op, num_elements, coord);
        }
    }

    /**
     * H5Sselect_all selects the entire extent of the dataspace space_id.
     * 
     * @param space_id IN: The identifier of the dataspace to be selected.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sselect_all(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_all(space_id);
        }
    }

    /**
     * H5Sselect_none resets the selection region for the dataspace space_id to include no elements.
     * 
     * @param space_id IN: The identifier of the dataspace to be reset.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sselect_none(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_none(space_id);
        }
    }

    /**
     * H5Sselect_valid verifies that the selection for the dataspace.
     * 
     * @param space_id The identifier for the dataspace in which the selection is being reset.
     * @return true if the selection is contained within the extent and FALSE if it is not or is an
     *         error.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static boolean H5Sselect_valid(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_valid(space_id);
        }
    }

    /**
     * H5Sget_simple_extent_npoints determines the number of elements in a dataspace.
     * 
     * @param space_id ID of the dataspace object to query
     * @return the number of elements in the dataspace if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Sget_simple_extent_npoints(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_simple_extent_npoints(space_id);
        }
    }

    /**
     * H5Sget_select_npoints determines the number of elements in the current selection of a
     * dataspace.
     * 
     * @param space_id Dataspace identifier.
     * @return the number of elements in the selection if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Sget_select_npoints(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_npoints(space_id);
        }
    }

    /**
     * H5Sget_simple_extent_ndims determines the dimensionality (or rank) of a dataspace.
     * 
     * @param space_id Identifier of the dataspace
     * @return the number of dimensions in the dataspace if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sget_simple_extent_ndims(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_simple_extent_ndims(space_id);
        }
    }

    /**
     * H5Sget_simple_extent_dims returns the size and maximum sizes of each dimension of a dataspace
     * through the dims and maxdims parameters.
     * 
     * @param space_id IN: Identifier of the dataspace object to query
     * @param dims OUT: Pointer to array to store the size of each dimension.
     * @param maxdims OUT: Pointer to array to store the maximum size of each dimension.
     * @return the number of dimensions in the dataspace if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - dims or maxdims is null.
     */
    public static int H5Sget_simple_extent_dims(int space_id, long[] dims, long[] maxdims)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_simple_extent_dims(space_id, dims, maxdims);
        }
    }

    /**
     * H5Sget_simple_extent_type queries a dataspace to determine the current class of a dataspace.
     * 
     * @param space_id Dataspace identifier.
     * @return a dataspace class name if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sget_simple_extent_type(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_simple_extent_type(space_id);
        }
    }

    /**
     * H5Sset_extent_simple sets or resets the size of an existing dataspace.
     * 
     * @param space_id Dataspace identifier.
     * @param rank Rank, or dimensionality, of the dataspace.
     * @param current_size Array containing current size of dataspace.
     * @param maximum_size Array containing maximum size of dataspace.
     * @return a dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sset_extent_simple(int space_id, int rank, byte[] current_size,
            byte[] maximum_size) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sset_extent_simple(space_id, rank, current_size, maximum_size);
        }
    }

    public static int H5Sset_extent_simple(final int space_id, final int rank,
            final long[] currentSize, final long[] maxSize) throws HDF5Exception,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sset_extent_simple(space_id, rank, currentSize, maxSize);
        }
    }

    /**
     * H5Sis_simple determines whether a dataspace is a simple dataspace.
     * 
     * @param space_id Identifier of the dataspace to query
     * @return true if is a simple dataspace
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static boolean H5Sis_simple(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sis_simple(space_id);
        }
    }

    /**
     * H5Soffset_simple sets the offset of a simple dataspace space_id.
     * 
     * @param space_id IN: The identifier for the dataspace object to reset.
     * @param offset IN: The offset at which to position the selection.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - offset array is null.
     */
    public static int H5Soffset_simple(int space_id, byte[] offset) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Soffset_simple(space_id, offset);
        }
    }

    public static int H5Soffset_simple(final int space_id, final long[] offset)
            throws HDF5Exception, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Soffset_simple(space_id, offset);
        }
    }

    /**
     * H5Sextent_copy copies the extent from source_space_id to dest_space_id. This action may
     * change the type of the dataspace.
     * 
     * @param dest_space_id IN: The identifier for the dataspace from which the extent is copied.
     * @param source_space_id IN: The identifier for the dataspace to which the extent is copied.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sextent_copy(int dest_space_id, int source_space_id)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sextent_copy(dest_space_id, source_space_id);
        }
    }

    /**
     * H5Sset_extent_none removes the extent from a dataspace and sets the type to H5S_NONE.
     * 
     * @param space_id The identifier for the dataspace from which the extent is to be removed.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sset_extent_none(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sset_extent_none(space_id);
        }
    }

    /**
     * H5Sselect_hyperslab selects a hyperslab region to add to the current selected region for the
     * dataspace specified by space_id. The start, stride, count, and block arrays must be the same
     * size as the rank of the dataspace.
     * 
     * @param space_id IN: Identifier of dataspace selection to modify
     * @param op IN: Operation to perform on current selection.
     * @param start IN: Offset of start of hyperslab
     * @param count IN: Number of blocks included in hyperslab.
     * @param stride IN: Hyperslab stride.
     * @param block IN: Size of block in hyperslab.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - an input array is null.
     * @exception NullPointerException - an input array is invalid.
     */
    public static int H5Sselect_hyperslab(int space_id, int op, byte[] start, byte[] stride,
            byte[] count, byte[] block) throws HDF5LibraryException, NullPointerException,
            IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_hyperslab(space_id, op, start, stride, count, block);
        }
    }

    public static int H5Sselect_hyperslab(final int space_id, final int op, final long[] start,
            final long[] stride, final long[] count, final long[] block) throws HDF5Exception,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sselect_hyperslab(space_id, op, start, stride, count, block);
        }
    }

    /**
     * H5Sclose releases a dataspace.
     * 
     * @param space_id Identifier of dataspace to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Sclose(int space_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sclose(space_id);
        }
    }

    /**
     * H5Sget_select_hyper_nblocks returns the number of hyperslab blocks in the current dataspace
     * selection.
     * 
     * @param spaceid Identifier of dataspace to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Sget_select_hyper_nblocks(int spaceid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_hyper_nblocks(spaceid);
        }
    }

    /**
     * H5Sget_select_elem_npoints returns the number of element points in the current dataspace
     * selection.
     * 
     * @param spaceid Identifier of dataspace to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Sget_select_elem_npoints(int spaceid) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_elem_npoints(spaceid);
        }
    }

    /**
     * H5Sget_select_hyper_blocklist returns an array of hyperslab blocks. The block coordinates
     * have the same dimensionality (rank) as the dataspace they are located within. The list of
     * blocks is formatted as follows:
     * 
     * <pre>
     * 
     * &lt;&quot;start&quot; coordinate&gt;, immediately followed by &lt;&quot;opposite&quot; corner
     * coordinate&gt;, followed by the next &quot;start&quot; and &quot;opposite&quot; coordinates,
     * etc. until all of the selected blocks have been listed.
     * 
     * </pre>
     * 
     * @param spaceid Identifier of dataspace to release.
     * @param startblock first block to retrieve
     * @param numblocks number of blocks to retrieve
     * @param buf returns blocks startblock to startblock+num-1, each block is <i>rank</i> * 2
     *            (corners) longs.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - buf is null.
     */
    public static int H5Sget_select_hyper_blocklist(int spaceid, long startblock, long numblocks,
            long[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_hyper_blocklist(spaceid, startblock, numblocks, buf);
        }
    }

    /**
     * H5Sget_select_elem_pointlist returns an array of of element points in the current dataspace
     * selection. The point coordinates have the same dimensionality (rank) as the dataspace they
     * are located within, one coordinate per point.
     * 
     * @param spaceid Identifier of dataspace to release.
     * @param startpoint first point to retrieve
     * @param numpoints number of points to retrieve
     * @param buf returns points startblock to startblock+num-1, each points is <i>rank</i> longs.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - buf is null.
     */
    public static int H5Sget_select_elem_pointlist(int spaceid, long startpoint, long numpoints,
            long[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_elem_pointlist(spaceid, startpoint, numpoints, buf);
        }
    }

    /**
     * H5Sget_select_bounds retrieves the coordinates of the bounding box containing the current
     * selection and places them into user-supplied buffers.
     * <P>
     * The start and end buffers must be large enough to hold the dataspace rank number of
     * coordinates.
     * 
     * @param spaceid Identifier of dataspace to release.
     * @param start coordinates of lowest corner of bounding box.
     * @param end coordinates of highest corner of bounding box.
     * @return a non-negative value if successful,with start and end initialized.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - start or end is null.
     */
    public static int H5Sget_select_bounds(int spaceid, long[] start, long[] end)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Sget_select_bounds(spaceid, start, end);
        }
    }

}
