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

/**
 * An object representing the storage features that are to be used for a data set.
 * <p>
 * The available storage layouts are {@link HDF5StorageLayout#COMPACT},
 * {@link HDF5StorageLayout#CONTIGUOUS} or {@link HDF5StorageLayout#CHUNKED} can be chosen. Only
 * {@link HDF5StorageLayout#CHUNKED} is extendable and can be compressed.
 * <p>
 * For generic (that is non-integer and non-float) data sets only one type of compression is
 * supported, which is <i>deflation</i>, the method used by <code>gzip</code>. The deflation level
 * can be chosen to get the right balance between speed of compression and compression ratio. Often
 * the {@link #DEFAULT_DEFLATION_LEVEL} will be the right choice.
 * 
 * @author Bernd Rinn
 */
public final class HDF5GenericStorageFeatures extends HDF5AbstractStorageFeatures
{
    /**
     * Represents 'no compression'.
     */
    public static final HDF5GenericStorageFeatures GENERIC_NO_COMPRESSION =
            new HDF5GenericStorageFeatures(null, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents a compact storage layout.
     */
    public static final HDF5GenericStorageFeatures GENERIC_COMPACT =
            new HDF5GenericStorageFeatures(HDF5StorageLayout.COMPACT, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a contiguous storage layout.
     */
    public static final HDF5GenericStorageFeatures GENERIC_CONTIGUOUS =
            new HDF5GenericStorageFeatures(HDF5StorageLayout.CONTIGUOUS, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents a chunked storage layout.
     */
    public static final HDF5GenericStorageFeatures GENERIC_CHUNKED =
            new HDF5GenericStorageFeatures(HDF5StorageLayout.CHUNKED, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     */
    public static final HDF5GenericStorageFeatures GENERIC_DEFLATE =
            new HDF5GenericStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     */
    public static final HDF5GenericStorageFeatures GENERIC_DEFLATE_MAX =
            new HDF5GenericStorageFeatures(null, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR);

    /**
     * Creates a {@link HDF5GenericStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5GenericStorageFeatures createDeflation(int deflationLevel)
    {
        if (deflationLevel == NO_DEFLATION_LEVEL)
        {
            return GENERIC_NO_COMPRESSION;
        } else if (deflationLevel == DEFAULT_DEFLATION_LEVEL)
        {
            return GENERIC_DEFLATE;
        } else if (deflationLevel == MAX_DEFLATION_LEVEL)
        {
            return GENERIC_DEFLATE_MAX;
        } else
        {
            return new HDF5GenericStorageFeatures(null, toByte(deflationLevel), NO_SCALING_FACTOR);
        }
    }

    /**
     * Legacy method for specifying the compression as a boolean value.
     */
    static HDF5GenericStorageFeatures getCompression(boolean deflate)
    {
        return deflate ? GENERIC_DEFLATE : GENERIC_NO_COMPRESSION;
    }

    HDF5GenericStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, byte deflateLevel, byte scalingFactor)
    {
        super(proposedLayoutOrNull, deflateLevel, scalingFactor);
    }

    /**
     * Returns true, if this compression setting can be applied on the given <var>dataClassId</var>.
     */
    @Override
    boolean isCompatibleWithDataClass(int dataClassId)
    {
        return true;
    }

}
