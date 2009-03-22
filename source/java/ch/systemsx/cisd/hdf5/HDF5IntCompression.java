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
 * An object representing the compression settings that are to be used for a float data set.
 * <p>
 * Currently two types of compressions are supported: <i>deflation</i> (the method used by
 * <code>gzip</code>) and <i>scaling</i>, which can be used if the accuracy of the values are
 * smaller than what the atomic data type can store. <b>Note that <i>scaling</i> can be a lossy
 * compression</b> while <i>deflation</i> is always lossless. <i>Scaling</i> compression is only
 * available with HDF5 1.8 and newer. Trying to use <i>scaling</i> in strict HDF5 1.6 compatibility
 * mode will throw an {@link IllegalStateException}.
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
public final class HDF5IntCompression extends HDF5AbstractCompression
{

    /**
     * Perform an automatic scaling on integer data.
     */
    private final static byte INTEGER_AUTO_SCALING_FACTOR = 0;

    /**
     * Represents 'no compression'.
     */
    public static final HDF5IntCompression INT_NO_COMPRESSION =
            new HDF5IntCompression(NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     */
    public static final HDF5IntCompression INT_DEFLATE =
            new HDF5IntCompression(DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     */
    public static final HDF5IntCompression INT_DEFLATE_MAX =
            new HDF5IntCompression(MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values.
     */
    public static final HDF5IntCompression INT_AUTO_SCALING =
            new HDF5IntCompression(NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     */
    public static final HDF5IntCompression INT_AUTO_SCALING_DEFLATE =
            new HDF5IntCompression(DEFAULT_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR);

    /**
     * Creates a {@link HDF5IntCompression} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5IntCompression createDeflation(int deflationLevel)
    {
        if (deflationLevel == NO_DEFLATION_LEVEL)
        {
            return INT_NO_COMPRESSION;
        } else if (deflationLevel == DEFAULT_DEFLATION_LEVEL)
        {
            return INT_DEFLATE;
        } else if (deflationLevel == MAX_DEFLATION_LEVEL)
        {
            return INT_DEFLATE_MAX;
        } else
        {
            return new HDF5IntCompression(toByte(deflationLevel), NO_SCALING_FACTOR);
        }
    }

    /**
     * Creates a {@link HDF5IntCompression} object that represents integer scaling with the given
     * <var>scalingFactor</var>.
     */
    public static HDF5IntCompression createIntegerScaling(int scalingFactor)
    {
        if (scalingFactor == INTEGER_AUTO_SCALING_FACTOR)
        {
            return INT_AUTO_SCALING_DEFLATE;
        } else
        {
            return new HDF5IntCompression(NO_DEFLATION_LEVEL, toByte(scalingFactor));
        }
    }

    /**
     * Creates a {@link HDF5IntCompression} object that represents deflation with the default
     * deflation level and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntCompression createDeflateAndIntegerScaling(int scalingFactor)
    {
        return new HDF5IntCompression(DEFAULT_DEFLATION_LEVEL, toByte(scalingFactor));
    }

    /**
     * Creates a {@link HDF5IntCompression} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntCompression createDeflateAndIntegerScaling(int deflateLevel,
            byte scalingFactor)
    {
        return new HDF5IntCompression(toByte(deflateLevel), scalingFactor);
    }

    HDF5IntCompression(byte deflateLevel, byte scalingFactor)
    {
        super(deflateLevel, scalingFactor);
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
