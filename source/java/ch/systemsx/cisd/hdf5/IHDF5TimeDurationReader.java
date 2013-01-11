/*
 * Copyright 2013 ETH Zuerich, CISD
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

/**
 * An interface that provides methods for reading time duration values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5TimeDurationReader
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Returns <code>true</code>, if the attribute <var>attributeName</var> of data set
     * <var>objectPath</var> is a time duration and <code>false</code> otherwise.
     */
    public boolean isTimeDuration(final String objectPath, String attributeName)
            throws HDF5JavaException;

    /**
     * Returns the time unit, if the attribute given by <var>attributeName</var> of object
     * <var>objectPath</var> is a time duration and <code>null</code> otherwise.
     */
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    /**
     * Reads a time duration attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDuration getAttr(final String objectPath, final String attributeName);

    /**
     * Reads a time duration array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray getArrayAttr(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimension time duration array attribute named <var>attributeName</var> from the
     * data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationMDArray getMDArrayAttr(final String objectPath,
            final String attributeName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     */
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Returns the time unit, if the data set given by <var>objectPath</var> is a time duration and
     * <code>null</code> otherwise.
     */
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time duration value and its unit from the data set <var>objectPath</var>. It needs to
     * be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, HDF5TimeDuration)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration and its unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDuration read(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>. It needs to be tagged as
     * one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, HDF5TimeDuration)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration in seconds.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray readArray(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, HDF5TimeDuration)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniently by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be
     *         <code>min(size - blockSize*blockNumber,
     *         blockSize)</code>.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray readArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, HDF5TimeDuration)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniently by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray readArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     */
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

}
