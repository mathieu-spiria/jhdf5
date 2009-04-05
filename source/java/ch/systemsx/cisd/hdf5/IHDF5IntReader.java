/*
 * Copyright 2009 ETH Zuerich, CISD.
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

package ch.systemsx.cisd.hdf5;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDIntArray;

/**
 * An interface that provides methods for reading <code>int</code> values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
interface IHDF5IntReader
{
    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>int</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public int getIntAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>int[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public int[] getIntArrayAttribute(final String objectPath, final String attributeName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>int</code> value from the data set <var>objectPath</var>. This method 
     * doesn't check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public int readInt(final String objectPath);

    /**
     * Reads a <code>int</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public int[] readIntArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>int</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     */
    public long[] readToIntMDArrayWithOffset(final String objectPath, 
    				final MDIntArray array, final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>int</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     */
    public long[] readToIntMDArrayBlockWithOffset(final String objectPath,
            final MDIntArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset);

    /**
     * Reads a block from a <code>int</code> array (of rank 1) from the data set 
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>int[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public int[] readIntArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block from <code>int</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>int[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with 0).
     * @return The data block read from the data set.
     */
    public int[] readIntArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     *
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrix(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     *
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY) 
            throws HDF5JavaException;

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     *
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrixBlockWithOffset(final String objectPath, 
    				final int blockSizeX, final int blockSizeY, final long offsetX, final long offsetY) 
    				throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDIntArray readIntMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set 
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDIntArray readIntMDArrayBlock(final String objectPath,
    				final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDIntArray readIntMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);
    
    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(
    									final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(
    									final String dataSetPath);
}
