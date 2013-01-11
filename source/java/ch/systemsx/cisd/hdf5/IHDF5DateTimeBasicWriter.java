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

/**
 * An interface that provides methods for writing time and date values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5DateTimeBasicWriter
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a date value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param date The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setDateAttribute(final String objectPath, final String attributeName,
            final Date date);

    /**
     * Set a date array value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param dates The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setDateArrayAttribute(final String objectPath, final String attributeName,
            final Date[] dates);

    /**
     * Set a time stamp value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeStamp The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setTimeStampAttribute(final String objectPath, final String attributeName,
            final long timeStamp);

    /**
     * Set a time stamp array value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeStamps The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setTimeStampArrayAttribute(final String objectPath, final String attributeName,
            final long[] timeStamps);

    /**
     * Set a time duration value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDuration The value of the attribute.
     * @param timeUnit The unit of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setTimeDurationAttribute(final String objectPath, final String attributeName,
            final long timeDuration, final HDF5TimeUnit timeUnit);

    /**
     * Set a time duration value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDuration The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setTimeDurationAttribute(final String objectPath, final String attributeName,
            final HDF5TimeDuration timeDuration);

    /**
     * Set a time duration array value as attribute on the referenced object. The smallest time unit
     * in <var>timeDurations</var> will be used as the time unit of the array.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * <p>
     * <em>Note: Time durations are stored as a <code>long[]</code> array.</em>
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDurations The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void setTimeDurationArrayAttribute(final String objectPath, final String attributeName,
            final HDF5TimeDurationArray timeDurations);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a time stamp value. The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamp The timestamp to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeStamp(final String objectPath, final long timeStamp);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final int size);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final long size, final int blockSize,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeStampArray(final String objectPath, final long[] timeStamps);

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeStampArray(final String objectPath, final long[] timeStamps,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link IHDF5LongWriter#createLongArray(String, long, int, HDF5IntStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeStampArrayBlock(final String objectPath, final long[] data,
            final long blockNumber);

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeTimeStampArrayBlock(String, long[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link IHDF5LongWriter#createLongArray(String, long, int, HDF5IntStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeStampArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a time stamp value provided as a {@link Date}.
     * <p>
     * <em>Note: The time stamp is stored as <code>long</code> array and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param date The date to write.
     * @see #writeTimeStamp(String, long)
     */
    public void writeDate(final String objectPath, final Date date);

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @see #writeTimeStampArray(String, long[])
     */
    public void writeDateArray(final String objectPath, final Date[] dates);

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time date is stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @param features The storage features of the data set.
     * @see #writeTimeStampArray(String, long[], HDF5GenericStorageFeatures)
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeDateArray(final String objectPath, final Date[] dates,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time duration value in seconds.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in seconds.
     * @deprecated Use {@link IHDF5TimeDurationWriter#write(String, HDF5TimeDuration)} instead.
     */
    @Deprecated
    public void writeTimeDuration(final String objectPath, final long timeDuration);

    /**
     * Writes out a time duration value.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeDuration(final String objectPath, final long timeDuration,
            final HDF5TimeUnit timeUnit);

    /**
     * Writes out a time duration value.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write.
     */
    public void writeTimeDuration(final String objectPath, final HDF5TimeDuration timeDuration);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final int size,
            final HDF5TimeUnit timeUnit);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final int size,
            final HDF5TimeUnit timeUnit, final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time duration array in seconds (of rank 1).
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in seconds.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)}
     *             instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)}
     *             instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit);

    /**
     * Writes out a time duration array (of rank 1). The smallest time unit in
     * <var>timeDurations</var> will be used as the time unit of the array.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>. The array
     *            will be stored in the smallest time unit, durations given in larger time units
     *            will be converted.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)} and
     *             {@link HDF5TimeDurationArray#create(HDF5TimeDuration...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray, HDF5IntStorageFeatures)
     *             )} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit, final HDF5IntStorageFeatures features);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>. The array
     *            will be stored in the smallest time unit, durations given in larger time units
     *            will be converted.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray, HDF5IntStorageFeatures)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations, final HDF5IntStorageFeatures features);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write.
     */
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDurationArray timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write.
     * @param features The storage features used to store the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDurationArray timeDurations, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a time duration array. The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath,
            final HDF5TimeDurationArray data, final long blockNumber);

    /**
     * Writes out a block of a time duration array. The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#times()} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDurationArray data, final int dataSize, final long offset);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlock(String, HDF5TimeDurationArray, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeUnit, long...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath, final long[] data,
            final long blockNumber, final HDF5TimeUnit timeUnit);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlockWithOffset(String, HDF5TimeDurationArray, int, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeUnit, long[])} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset, final HDF5TimeUnit timeUnit);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlock(String, HDF5TimeDurationArray, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath, final HDF5TimeDuration[] data,
            final long blockNumber);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlockWithOffset(String, HDF5TimeDurationArray, int, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration[])}.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDuration[] data, final int dataSize, final long offset);

}