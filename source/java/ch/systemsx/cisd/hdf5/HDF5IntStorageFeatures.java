/*
 * Copyright 2009 ETH Zuerich, CISD
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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_INTEGER;

/**
 * An object representing the storage features that are to be used for an integer data set.
 * <p>
 * The <code>..._KEEP</code> variants denote that the specified storage features should only be
 * applied if a new data set has to be created. If the data set already exists, it will be kept with
 * whatever storage features it has.
 * <em>Note that this may lead to an exception if the existing data set is non-extendable and the 
 * dimensions of the new data set differ from the dimensions of the existing data set.</em>
 * <p>
 * The available storage layouts are {@link HDF5StorageLayout#COMPACT},
 * {@link HDF5StorageLayout#CONTIGUOUS} or {@link HDF5StorageLayout#CHUNKED} can be chosen. Only
 * {@link HDF5StorageLayout#CHUNKED} is extendable and can be compressed.
 * <p>
 * Two types of compressions are supported: <i>deflation</i> (the method used by <code>gzip</code>)
 * and <i>scaling</i>, which can be used if the accuracy of the values are smaller than what the
 * atomic data type can store. <b>Note that <i>scaling</i> can be a lossy compression</b> while
 * <i>deflation</i> is always lossless. <i>Scaling</i> compression is only available with HDF5 1.8
 * and newer. Trying to use <i>scaling</i> in strict HDF5 1.6 compatibility mode will throw an
 * {@link IllegalStateException}.
 * <p>
 * For <i>deflation</i> the deflation level can be chosen to get the right balance between speed of
 * compression and compression ratio. Often the {@link #DEFAULT_DEFLATION_LEVEL} will be the right
 * choice.
 * <p>
 * For <i>scaling</i>, the scaling factor can be chosen that determines the accuracy of the values
 * saved. For float values, the scaling factor determines the number of significant digits of the
 * numbers. The algorithm used for scale compression is:
 * <ol>
 * <li>Calculate the minimum value of all values</li>
 * <li>Subtract the minimum value from all values</li>
 * <li>Store the number of bits specified as <code>scalingFactor</code></li>
 * </ol>
 * Note that this compression is lossless if
 * <code>{@literal scalineFactor >= ceil(log2(max(values) - min(values) + 1)}</code>. This in made
 * sure when using {@link #INT_AUTO_SCALING}, thus {@link #INT_AUTO_SCALING} is always losless.
 * <p>
 * <b>Contrary to float scaling compression, a lossy integer scaling compression is usually an error
 * as the most significant bits are chopped of!</b> The option to specify the scaling factor is
 * meant to give you a way to use that you <i>know</i> the span of the values
 * <code>{@literal max(values) - min(values)}</code> rather than asking the library to waste time on
 * computing it for you.
 * 
 * @author Bernd Rinn
 */
public final class HDF5IntStorageFeatures extends HDF5AbstractStorageFeatures
{

    /**
     * Perform an automatic scaling on integer data.
     */
    private final static byte INTEGER_AUTO_SCALING_FACTOR = 0;

    /**
     * Represents 'no compression', use default storage layout.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION =
            new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'no compression', use default storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_KEEP =
            new HDF5IntStorageFeatures(null, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents a compact storage layout.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT =
            new HDF5IntStorageFeatures(HDF5StorageLayout.COMPACT, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a compact storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.COMPACT, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a contiguous storage layout.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CONTIGUOUS, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a contiguous storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CONTIGUOUS, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a chunked (extendable) storage layout.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CHUNKED, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a chunked (extendable) storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CHUNKED, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE =
            new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_KEEP =
            new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX =
            new HDF5IntStorageFeatures(null, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_KEEP =
            new HDF5IntStorageFeatures(null, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING =
            new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_KEEP =
            new HDF5IntStorageFeatures(null, true, NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE =
            new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_KEEP =
            new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5IntStorageFeatures createDeflation(int deflationLevel)
    {
        return createDeflation(deflationLevel, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createDeflationKepp(int deflationLevel)
    {
        return createDeflation(deflationLevel, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    private static HDF5IntStorageFeatures createDeflation(int deflationLevel,
            boolean keepExistingDataSetIfExists)
    {
        if (deflationLevel == NO_DEFLATION_LEVEL)
        {
            return keepExistingDataSetIfExists ? INT_NO_COMPRESSION_KEEP : INT_NO_COMPRESSION;
        } else if (deflationLevel == DEFAULT_DEFLATION_LEVEL)
        {
            return keepExistingDataSetIfExists ? INT_DEFLATE_KEEP : INT_DEFLATE;
        } else if (deflationLevel == MAX_DEFLATION_LEVEL)
        {
            return keepExistingDataSetIfExists ? INT_DEFLATE_MAX_KEEP : INT_DEFLATE_MAX;
        } else
        {
            return new HDF5IntStorageFeatures(null, keepExistingDataSetIfExists,
                    toByte(deflationLevel), NO_SCALING_FACTOR);
        }
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createIntegerScaling(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createIntegerScalingKepp(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     */
    private static HDF5IntStorageFeatures createIntegerScaling(int scalingFactor,
            boolean keepExistingDataSetIfExists)
    {
        if (scalingFactor == INTEGER_AUTO_SCALING_FACTOR)
        {
            return keepExistingDataSetIfExists ? INT_AUTO_SCALING_DEFLATE_KEEP
                    : INT_AUTO_SCALING_DEFLATE;
        } else
        {
            return new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, toByte(scalingFactor));
        }
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the default
     * deflation level and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScaling(int scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the default
     * deflation level and integer scaling with the given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScalingKeep(int scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL,
                toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScaling(int deflateLevel,
            byte scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, toByte(deflateLevel), scalingFactor);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScalingKeep(int deflateLevel,
            byte scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, true, toByte(deflateLevel), scalingFactor);
    }

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, byte deflateLevel,
            byte scalingFactor)
    {
        this(proposedLayoutOrNull, false, deflateLevel, scalingFactor);
    }

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, boolean keepDataSetIfExists,
            byte deflateLevel, byte scalingFactor)
    {
        super(proposedLayoutOrNull, keepDataSetIfExists, deflateLevel, scalingFactor);
    }

    /**
     * Returns true, if this compression setting can be applied on the given <var>dataClassId</var>.
     */
    @Override
    boolean isCompatibleWithDataClass(int dataClassId)
    {
        return (dataClassId == H5T_INTEGER);
    }

}
