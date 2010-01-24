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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_FLOAT;

/**
 * An object representing the storage features that are to be used for a float data set.
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
 * atomic data type can store. <b>Note that <i>scaling</i> in general is a lossy compression</b>
 * while <i>deflation</i> is always lossless. <i>Scaling</i> compression is only available with HDF5
 * 1.8 and newer. Trying to use <i>scaling</i> in strict HDF5 1.6 compatibility mode will throw an
 * {@link IllegalStateException}.
 * <p>
 * For <i>deflation</i> the deflation level can be chosen to get the right balance between speed of
 * compression and compression ratio. Often the {@link #DEFAULT_DEFLATION_LEVEL} will be the right
 * choice.
 * <p>
 * For <i>scaling</i>, the scaling factor can be chosen that determines the accuracy of the values
 * saved. For float values, the scaling factor determines the number of significant digits of the
 * numbers. It is guaranteed that <code>{@literal |f_real - f_saved| < 10^(-scalingFactor)}</code>.
 * The algorithm used for scale compression is:
 * <ol>
 * <li>Calculate the minimum value of all values</li>
 * <li>Subtract the minimum value from all values</li>
 * <li>Multiply all values obtained in step 2 with <code>{@literal 10^scalingFactor}</code></li>
 * <li>Round the values obtained in step 3 to the nearest integer value</li>
 * <li>Store the minimum found in step 1 and the values obtained in step 4</li>
 * </ol>
 * This algorithm is known as GRIB D-scaling.
 * 
 * @author Bernd Rinn
 */
public final class HDF5FloatStorageFeatures extends HDF5AbstractStorageFeatures
{

    /**
     * Represents 'no compression', use default storage layout.
     */
    public static final HDF5FloatStorageFeatures FLOAT_NO_COMPRESSION =
            new HDF5FloatStorageFeatures(null, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'no compression', use default storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_NO_COMPRESSION_KEEP =
            new HDF5FloatStorageFeatures(null, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents a compact storage layout.
     */
    public static final HDF5FloatStorageFeatures FLOAT_COMPACT =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.COMPACT, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a compact storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_COMPACT_KEEP =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.COMPACT, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a contiguous storage layout.
     */
    public static final HDF5FloatStorageFeatures FLOAT_CONTIGUOUS =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.CONTIGUOUS, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a contiguous storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_CONTIGUOUS_KEEP =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.CONTIGUOUS, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a chunked storage layout.
     */
    public static final HDF5FloatStorageFeatures FLOAT_CHUNKED =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.CHUNKED, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a chunked storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_CHUNKED_KEEP =
            new HDF5FloatStorageFeatures(HDF5StorageLayout.CHUNKED, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     */
    public static final HDF5FloatStorageFeatures FLOAT_DEFLATE =
            new HDF5FloatStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_DEFLATE_KEEP =
            new HDF5FloatStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     */
    public static final HDF5FloatStorageFeatures FLOAT_DEFLATE_MAX =
            new HDF5FloatStorageFeatures(null, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_DEFLATE_MAX_KEEP =
            new HDF5FloatStorageFeatures(null, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents scaling with scaling factor 1 for float values.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING1 =
            new HDF5FloatStorageFeatures(null, NO_DEFLATION_LEVEL, (byte) 1);

    /**
     * Represents scaling with scaling factor 1 for float values.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING1_KEEP =
            new HDF5FloatStorageFeatures(null, true, NO_DEFLATION_LEVEL, (byte) 1);

    /**
     * Represents scaling with scaling factor 1 for float values combined with deflation using the
     * default deflation level.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING1_DEFLATE =
            new HDF5FloatStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, (byte) 1);

    /**
     * Represents scaling with scaling factor 1 for float values combined with deflation using the
     * default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING1_DEFLATE_KEEP =
            new HDF5FloatStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, (byte) 1);

    /**
     * Represents scaling with scaling factor 2 for float values.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING2 =
            new HDF5FloatStorageFeatures(null, NO_DEFLATION_LEVEL, (byte) 2);

    /**
     * Represents scaling with scaling factor 2 for float values.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING2_KEEP =
            new HDF5FloatStorageFeatures(null, true, NO_DEFLATION_LEVEL, (byte) 2);

    /**
     * Represents scaling with scaling factor 2 for float values combined with deflation using the
     * default deflation level.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING2_DEFLATE =
            new HDF5FloatStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, (byte) 2);

    /**
     * Represents scaling with scaling factor 2 for float values combined with deflation using the
     * default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING2_DEFLATE_KEEP =
            new HDF5FloatStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, (byte) 2);

    /**
     * Represents scaling with scaling factor 3 for float values.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING3 =
            new HDF5FloatStorageFeatures(null, NO_DEFLATION_LEVEL, (byte) 3);

    /**
     * Represents scaling with scaling factor 3 for float values.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING3_KEEP =
            new HDF5FloatStorageFeatures(null, true, NO_DEFLATION_LEVEL, (byte) 3);

    /**
     * Represents scaling with scaling factor 3 for float values combined with deflation using the
     * default deflation level.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING3_DEFLATE =
            new HDF5FloatStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, (byte) 3);

    /**
     * Represents scaling with scaling factor 3 for float values combined with deflation using the
     * default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5FloatStorageFeatures FLOAT_SCALING3_DEFLATE_KEEP =
            new HDF5FloatStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, (byte) 3);

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5FloatStorageFeatures createDeflation(int deflationLevel)
    {
        return createDeflation(deflationLevel, false);
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5FloatStorageFeatures createDeflationKeep(int deflationLevel)
    {
        return createDeflation(deflationLevel, true);
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    private static HDF5FloatStorageFeatures createDeflation(int deflationLevel,
            boolean keepDataSetIfExists)
    {
        if (deflationLevel == NO_DEFLATION_LEVEL)
        {
            return keepDataSetIfExists ? FLOAT_NO_COMPRESSION_KEEP : FLOAT_NO_COMPRESSION;
        } else if (deflationLevel == DEFAULT_DEFLATION_LEVEL)
        {
            return keepDataSetIfExists ? FLOAT_DEFLATE_KEEP : FLOAT_DEFLATE;
        } else if (deflationLevel == MAX_DEFLATION_LEVEL)
        {
            return keepDataSetIfExists ? FLOAT_DEFLATE_MAX_KEEP : FLOAT_DEFLATE_MAX;
        } else
        {
            return new HDF5FloatStorageFeatures(null, keepDataSetIfExists, toByte(deflationLevel),
                    NO_SCALING_FACTOR);
        }
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents float scaling with the
     * given <var>scalingFactor</var>.
     */
    public static HDF5FloatStorageFeatures createFloatScaling(int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, NO_DEFLATION_LEVEL, toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents float scaling with the
     * given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5FloatStorageFeatures createFloatScalingKeep(int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, true, NO_DEFLATION_LEVEL, toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the default
     * deflation level and float scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5FloatStorageFeatures createDeflateAndFloatScaling(int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the default
     * deflation level and float scaling with the given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5FloatStorageFeatures createDeflateAndFloatScalingKeep(int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL,
                toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and float scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5FloatStorageFeatures createDeflateAndFloatScaling(int deflateLevel,
            int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, toByte(deflateLevel), toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5FloatStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and float scaling with the given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5FloatStorageFeatures createDeflateAndFloatScalingKeep(int deflateLevel,
            int scalingFactor)
    {
        return new HDF5FloatStorageFeatures(null, true, toByte(deflateLevel), toByte(scalingFactor));
    }

    HDF5FloatStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, byte deflateLevel,
            byte scalingFactor)
    {
        this(proposedLayoutOrNull, false, deflateLevel, scalingFactor);
    }

    HDF5FloatStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, boolean keepDataSetIfExists,
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
        return (dataClassId == H5T_FLOAT);
    }

}
