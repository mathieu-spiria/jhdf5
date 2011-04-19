/*
 * Copyright 2011 ETH Zuerich, CISD
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

import ch.systemsx.cisd.base.mdarray.MDArray;

/**
 * An interface for reading references in HDF5 files.
 * 
 * @see IHDF5ReferenceWriter
 * @author Bernd Rinn
 */
public interface IHDF5ReferenceReader
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads an object reference attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     */
    public String getObjectReferenceAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a 1D object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     */
    public String[] getObjectReferenceArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads an object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     */
    public MDArray<String> getObjectReferenceMDArrayAttribute(final String objectPath,
            final String attributeName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads an object reference from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     */
    public String readObjectReference(final String objectPath);

    /**
     * Reads an array of object references from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The array of the paths of objects that the references refers to. Each string may be
     *         empty, if the corresponding object reference refers to an unnamed object.
     */
    public String[] readObjectReferenceArray(final String objectPath);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The referenced data set paths read from the data set. The length will be min(size -
     *         blockSize*blockNumber, blockSize).
     */
    public String[] readObjectReferenceArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with 0).
     * @return The referenced data set paths block read from the data set.
     */
    public String[] readObjectReferenceArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset);

    /**
     * Reads an array (or rank N) of object references from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The multi-dimensional array of the paths of objects that the references refers to.
     *         Each string may be empty, if the corresponding object reference refers to an unnamed
     *         object.
     */
    public MDArray<String> readObjectReferenceMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional array of object references from the data set 
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The referenced data set paths block read from the data set.
     */
    public MDArray<String> readObjectReferenceMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a multi-dimensional array of object references from the data set 
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The referenced data set paths block read from the data set.
     */
    public MDArray<String> readObjectReferenceMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<String[]>> getObjectReferenceArrayNaturalBlocks(
            final String dataSetPath);

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getObjectReferenceMDArrayNaturalBlocks(
            final String dataSetPath);
}
