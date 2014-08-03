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
 * Low-level interface for HDF5 dataset functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5D
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Dcreate creates a data set with a name, name, in the file or in the group specified by the
     * identifier loc_id.
     * 
     * @param loc_id Identifier of the file or group to create the dataset within.
     * @param name The name of the dataset to create.
     * @param type_id Identifier of the datatype to use when creating the dataset.
     * @param space_id Identifier of the dataspace to use when creating the dataset.
     * @param link_create_plist_id Identifier of the link creation property list.
     * @param dset_create_plist_id Identifier of the dataset creation property list.
     * @param dset_access_plist_id Identifier of the dataset access property list.
     * @return a dataset identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Dcreate(int loc_id, String name, int type_id, int space_id,
            int link_create_plist_id, int dset_create_plist_id, int dset_access_plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dcreate(loc_id, name, type_id, space_id, link_create_plist_id,
                    dset_create_plist_id, dset_access_plist_id);

        }
    }

    /**
     * H5Dopen opens an existing dataset for access in the file or group specified in loc_id.
     * 
     * @param loc_id Identifier of the dataset to open or the file or group
     * @param name The name of the dataset to access.
     * @param access_plist_id Dataset access property list.
     * @return a dataset identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Dopen(int loc_id, String name, int access_plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dopen(loc_id, name, access_plist_id);
        }
    }

    public static int H5Dchdir_ext(String dir_name) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dchdir_ext(dir_name);
        }
    }

    public static int H5Dgetdir_ext(String[] dir_name, int size) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dgetdir_ext(dir_name, size);
        }
    }

    /**
     * H5Dget_space returns an identifier for a copy of the dataspace for a dataset.
     * 
     * @param dataset_id Identifier of the dataset to query.
     * @return a dataspace identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Dget_space(int dataset_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dget_space(dataset_id);
        }
    }

    /**
     * H5Dget_type returns an identifier for a copy of the datatype for a dataset.
     * 
     * @param dataset_id Identifier of the dataset to query.
     * @return a datatype identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Dget_type(int dataset_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dget_type(dataset_id);
        }
    }

    /**
     * H5Dget_create_plist returns an identifier for a copy of the dataset creation property list
     * for a dataset.
     * 
     * @param dataset_id Identifier of the dataset to query.
     * @return a dataset creation property list identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Dget_create_plist(int dataset_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dget_create_plist(dataset_id);
        }
    }

    /**
     * H5Dread reads a (partial) dataset, specified by its identifier dataset_id, from the file into
     * the application memory buffer buf.
     * 
     * @param dataset_id Identifier of the dataset read from.
     * @param mem_type_id Identifier of the memory datatype.
     * @param mem_space_id Identifier of the memory dataspace.
     * @param file_space_id Identifier of the dataset's dataspace in the file.
     * @param xfer_plist_id Identifier of a transfer property list for this I/O operation.
     * @param buf Buffer to store data read from the file.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - data buffer is null.
     */
    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, byte[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5DreadVL(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, Object[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5DreadVL(dataset_id, mem_type_id, mem_space_id, file_space_id,
                    xfer_plist_id, buf);
        }
    }

    /**
     * H5DwriteString writes a (partial) variable length String dataset, specified by its identifier
     * dataset_id, from the application memory buffer buf into the file.
     * <p>
     * <i>contributed by Rosetta Biosoftware.</i>
     * 
     * @param dataset_id Identifier of the dataset read from.
     * @param mem_type_id Identifier of the memory datatype.
     * @param mem_space_id Identifier of the memory dataspace.
     * @param file_space_id Identifier of the dataset's dataspace in the file.
     * @param xfer_plist_id Identifier of a transfer property list for this I/O operation.
     * @param buf Buffer with data to be written to the file.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5DwriteString(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, String[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5DwriteString(dataset_id, mem_type_id, mem_space_id, file_space_id,
                    xfer_plist_id, buf);
        }
    }

    /**
     * H5Dwrite writes a (partial) dataset, specified by its identifier dataset_id, from the
     * application memory buffer buf into the file.
     * 
     * @param dataset_id Identifier of the dataset read from.
     * @param mem_type_id Identifier of the memory datatype.
     * @param mem_space_id Identifier of the memory dataspace.
     * @param file_space_id Identifier of the dataset's dataspace in the file.
     * @param xfer_plist_id Identifier of a transfer property list for this I/O operation.
     * @param buf Buffer with data to be written to the file.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, byte[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    /**
     * H5Dextend verifies that the dataset is at least of size size.
     * 
     * @param dataset_id Identifier of the dataset.
     * @param size Array containing the new magnitude of each dimension.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size array is null.
     */
    public static int H5Dextend(int dataset_id, byte[] size) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dextend(dataset_id, size);
        }
    }

    public static int H5Dextend(final int dataset_id, final long[] size) throws HDF5Exception,
            NullPointerException
    {
        final byte[] buf = HDFNativeData.longToByte(size);

        return H5Dextend(dataset_id, buf);
    }

    /**
     * H5Dset_extent sets the size of the dataset to <var>size</var>. Make sure that no important
     * are lost since this method will not check that the data dimensions are not larger than
     * <var>size</var>.
     * 
     * @param dataset_id Identifier of the dataset.
     * @param size Array containing the new magnitude of each dimension.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - size array is null.
     */
    public static int H5Dset_extent(int dataset_id, byte[] size) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dset_extent(dataset_id, size);
        }
    }

    public static int H5Dset_extent(final int dataset_id, final long[] size) throws HDF5Exception,
            NullPointerException
    {
        final byte[] buf = HDFNativeData.longToByte(size);

        return H5Dset_extent(dataset_id, buf);
    }

    /**
     * H5Dclose ends access to a dataset specified by dataset_id and releases resources used by it.
     * 
     * @param dataset_id Identifier of the dataset to finish access to.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Dclose(int dataset_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dclose(dataset_id);
        }
    }

    /**
     * H5Dget_storage_size returns the amount of storage that is required for the dataset.
     * 
     * @param dataset_id Identifier of the dataset in question
     * @return he amount of storage space allocated for the dataset.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Dget_storage_size(int dataset_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dget_storage_size(dataset_id);
        }
    }

    /**
     * H5Dcopy copies the content of one dataset to another dataset.
     * 
     * @param src_did the identifier of the source dataset
     * @param dst_did the identifier of the destinaiton dataset
     */
    public static int H5Dcopy(int src_did, int dst_did) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dcopy(src_did, dst_did);
        }
    }

    /*
     *
     */
    public static int H5Dvlen_get_buf_size(int dataset_id, int type_id, int space_id, int[] size)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dvlen_get_buf_size(dataset_id, type_id, space_id, size);
        }
    }

    /**
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - buf is null.
     */
    public static int H5Dvlen_reclaim(int type_id, int space_id, int xfer_plist_id, byte[] buf)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dvlen_reclaim(type_id, space_id, xfer_plist_id, buf);
        }
    }

    public static int H5Dget_space_status(int dset_id, int[] status) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dget_space_status(dset_id, status);
        }
    }

    // //////////////////////////////////////////////////////////////////
    // //
    // New APIs for read data from library //
    // Using H5Dread(..., Object buf) requires function calls //
    // theArray.emptyBytes() and theArray.arrayify( buf), which //
    // triples the actual memory needed by the data set. //
    // Using the following APIs solves the problem. //
    // //
    // //////////////////////////////////////////////////////////////////

    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, short[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, int[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, long[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, float[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dread(int dataset_id, int mem_type_id, int mem_space_id, int file_space_id,
            int xfer_plist_id, double[] buf) throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dread_string(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, String[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread_string(dataset_id, mem_type_id, mem_space_id, file_space_id,
                    xfer_plist_id, buf);
        }
    }

    public static int H5Dread_reg_ref(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, String[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dread_reg_ref(dataset_id, mem_type_id, mem_space_id, file_space_id,
                    xfer_plist_id, buf);
        }
    }

    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, short[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, int[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, long[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, float[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

    public static int H5Dwrite(int dataset_id, int mem_type_id, int mem_space_id,
            int file_space_id, int xfer_plist_id, double[] buf) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Dwrite(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id,
                    buf);
        }
    }

}
