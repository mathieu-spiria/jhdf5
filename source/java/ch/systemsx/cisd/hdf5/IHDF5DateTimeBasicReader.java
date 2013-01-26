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

import java.util.Date;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * An interface that provides methods for reading time and date values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5DateTimeBasicReader
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Returns <code>true</code>, if the attribute <var>attributeName</var> of data set
     * <var>objectPath</var> is a time stamp and <code>false</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public boolean isTimeStamp(final String objectPath, String attributeName)
            throws HDF5JavaException;

    /**
     * Reads a time stamp attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long getTimeStampAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp array; each element is a number of milliseconds since January 1, 1970,
     *         00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] getTimeStampArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var> and returns it as a <code>Date</code>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public Date getDateAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var> and returns it as a <code>Date[]</code>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public Date[] getDateArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Returns <code>true</code>, if the attribute <var>attributeName</var> of data set
     * <var>objectPath</var> is a time duration and <code>false</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public boolean isTimeDuration(final String objectPath, String attributeName)
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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDuration getTimeDurationAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads a time duration array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray getTimeDurationArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Returns the time unit, if the attribute given by <var>attributeName</var> of object
     * <var>objectPath</var> is a time duration and <code>null</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time stamp and
     * <code>false</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException;

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Returns the time unit, if the data set given by <var>objectPath</var> is a time duration and
     * <code>null</code> otherwise.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time stamp value from the data set <var>objectPath</var>. The time stamp is stored as
     * a <code>long</code> value in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long readTimeStamp(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time stamp array from the data set <var>objectPath</var>. The time stamp is stored as
     * a <code>long</code> value in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] readTimeStampArray(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] readTimeStampArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] readTimeStampArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<long[]>> getTimeStampArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Reads a time stamp value from the data set <var>objectPath</var> and returns it as a
     * {@link Date}. The time stamp is stored as a <code>long</code> value in the HDF5 file. It
     * needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as {@link Date}.
     * @throws HDF5JavaException If the <var>objectPath</var> does not denote a time stamp.
     */
    public Date readDate(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time stamp array (of rank 1) from the data set <var>objectPath</var> and returns it
     * as an array of {@link Date}s. The time stamp array is stored as a an array of
     * <code>long</code> values in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as {@link Date}.
     * @throws HDF5JavaException If the <var>objectPath</var> does not denote a time stamp.
     */
    public Date[] readDateArray(final String objectPath) throws HDF5JavaException;

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
    public HDF5TimeDuration readTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time duration value and its unit from the data set <var>objectPath</var>, converts it
     * to the given <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged
     * as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration and its unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#read(String)} instead.
     */
    @Deprecated
    public HDF5TimeDuration readTimeDurationAndUnit(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads a time duration value from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged as one of the
     * type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The time duration in the given unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#read(String)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDuration)} instead.
     */
    @Deprecated
    public long readTimeDuration(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>. It needs to be tagged as
     * one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration in seconds.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray readTimeDurationArray(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>and returns it as a
     * <code>HDF5TimeDuration[]</code>. It needs to be tagged as one of the type variants that
     * indicate a time duration, for example {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time durations in their respective time unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @see #readTimeDurationArray(String, HDF5TimeUnit)
     * @deprecated Use {@link IHDF5TimeDurationReader#readArray(String)} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArray(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as a <code>long[]</code> array. It needs to be tagged as
     * one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The time duration in the given unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArray(String)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDurationArray)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArray(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray readTimeDurationArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>long[]</code> value in the HDF5 file. It needs to be
     * tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data read from the data set. The length will be
     *         <code>min(size - blockSize*blockNumber,
     *         blockSize)</code>.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlock(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(long[], HDF5TimeUnit)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final HDF5TimeUnit timeUnit) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>long[]</code> value in the HDF5 file. It needs to be
     * tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlockWithOffset(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDurationArray)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>HDF5TimeDuration[]</code> value in the HDF5 file. It
     * needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
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
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlock(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>HDF5TimeDuration[]</code> value in the HDF5 file. It
     * needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlockWithOffset(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getTimeDurationArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use {@link IHDF5TimeDurationReader#getArrayNaturalBlocks(String)} and
     *             {@link HDF5TimeUnit#convert(long[], HDF5TimeUnit)} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(
            final String objectPath, final HDF5TimeUnit timeUnit) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use {@link IHDF5TimeDurationReader#getArrayNaturalBlocks(String)} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<HDF5TimeDuration[]>> getTimeDurationAndUnitArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

}