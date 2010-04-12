/*
 * Copyright 2010 ETH Zuerich, CISD
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

import ch.systemsx.cisd.base.mdarray.MDArray;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * An interface that provides methods for reading <code>String</code> values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5StringReader
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>String</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public String getStringAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>String[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public String[] getStringArrayAttribute(final String objectPath, final String attributeName);
    
    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>String</code> from the data set <var>objectPath</var>. This needs to be a
     * string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String readString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a <code>String</code> array (of rank 1) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArray(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a block of a <code>String</code> array (of rank 1) from the data set
     * <var>objectPath</var>. The elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block to read from the data set.
     * @param blockNumber The number of the block to read from the data set (the offset is
     *            <code>blockSize * blockNumber</code>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block of a <code>String</code> array (of rank 1) from the data set
     * <var>objectPath</var>. The elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block to read from the data set.
     * @param offset The offset of the block in the data set.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>String</code> array (of rank N) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public MDArray<String> readStringMDArray(final String objectPath);
    
    /**
     * Reads a block of a <code>String</code> array (of rank N) from the data set
     * <var>objectPath</var>. The elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The dimensions (along each axis) of the block to read from the data
     *            set.
     * @param blockNumber The number of the block to read from the data set (the offset in each
     *            dimension i is <code>blockSize[i] * blockNumber[i]</code>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public MDArray<String> readStringMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a block of a <code>String</code> array (of rank N) from the data set
     * <var>objectPath</var>. The elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The dimensions (along each axis) of the block to read from the data
     *            set.
     * @param offset The offset of the block in the data set.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public MDArray<String> readStringMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<String[]>> getStringArrayNaturalBlocks(final String objectPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getStringMDArrayNaturalBlocks(
            final String objectPath);

}
