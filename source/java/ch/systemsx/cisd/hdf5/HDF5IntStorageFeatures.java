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

import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_INTEGER;

/**
 * An object representing the storage features that are to be used for an integer data set.
 * <p>
 * The <code>..._KEEP</code> variants denote that the specified storage features should only be
 * applied if a new data set has to be created. If the data set already exists, it will be kept with
 * whatever storage features it has.
 * <em>Note that this may lead to an exception if the existing data set is non-extendable and the 
 * dimensions of the new data set differ from the dimensions of the existing data set.</em>
 * <p>
 * The <code>..._DELETE</code> variants denote that the specified storage features should always be
 * applied. If the data set already exists, it will be deleted before the new data set is written.
 * This is the default behavior. However, if the
 * {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()} setting is given, the
 * <code>..._DELETE</code> variant can be used to override this setting on a case-by-case basis.
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
     * Represents 'no compression', signed integers, use default storage layout.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION = new HDF5IntStorageFeatures(
            null, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'no compression', unsigned integers, use default storage layout.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_UNSIGNED =
            new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents 'no compression', use default storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_KEEP =
            new HDF5IntStorageFeatures(null, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'no compression', unsigned integers, use default storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(null, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents 'no compression', use default storage layout.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_DELETE =
            new HDF5IntStorageFeatures(null, false, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR,
                    true);

    /**
     * Represents 'no compression', unsigned integers, use default storage layout.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_NO_COMPRESSION_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(null, false, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR,
                    false);

    /**
     * Represents a compact storage layout.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT = new HDF5IntStorageFeatures(
            HDF5StorageLayout.COMPACT, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a compact storage layout with unsigned integers.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_UNSIGNED = new HDF5IntStorageFeatures(
            HDF5StorageLayout.COMPACT, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents a compact storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_KEEP = new HDF5IntStorageFeatures(
            HDF5StorageLayout.COMPACT, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a compact storage layout with unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.COMPACT, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents a compact storage layout.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_DELETE = new HDF5IntStorageFeatures(
            HDF5StorageLayout.COMPACT, false, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a compact storage layout with unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_COMPACT_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(HDF5StorageLayout.COMPACT, false, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents a contiguous storage layout.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CONTIGUOUS, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a contiguous storage layout with unsigned integers.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_UNSIGNED =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CONTIGUOUS, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents a contiguous storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_KEEP = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CONTIGUOUS, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a contiguous storage layout with unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CONTIGUOUS, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents a contiguous storage layout.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_DELETE = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CONTIGUOUS, false, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a contiguous storage layout with unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_CONTIGUOUS_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CONTIGUOUS, false, true,
                    NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents a chunked (extendable) storage layout.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CHUNKED, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a chunked (extendable) storage layout with unsigned integers.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_UNSIGNED = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CHUNKED, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents a chunked (extendable) storage layout.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_KEEP = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CHUNKED, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a chunked (extendable) storage layout with unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CHUNKED, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents a chunked (extendable) storage layout.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_DELETE = new HDF5IntStorageFeatures(
            HDF5StorageLayout.CHUNKED, false, true, NO_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents a chunked (extendable) storage layout with unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_CHUNKED_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(HDF5StorageLayout.CHUNKED, false, true, NO_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE = new HDF5IntStorageFeatures(null,
            DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'standard compression' with a pre-filter shuffle, that is deflation with the
     * default deflation level.
     */
    public static final HDF5IntStorageFeatures INT_SHUFFLE_DEFLATE = new HDF5IntStorageFeatures(
            null, false, false, true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'standard compression' with unsigned integers, that is deflation with the default
     * deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_UNSIGNED = new HDF5IntStorageFeatures(
            null, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_KEEP = new HDF5IntStorageFeatures(null,
            true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'standard compression' with unsigned integers, that is deflation with the default
     * deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR,
                    false);

    /**
     * Represents 'standard compression', that is deflation with the default deflation level.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_DELETE = new HDF5IntStorageFeatures(
            null, false, true, DEFAULT_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'standard compression' with unsigned integers, that is deflation with the default
     * deflation level.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(null, false, true, DEFAULT_DEFLATION_LEVEL,
                    NO_SCALING_FACTOR, false);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX = new HDF5IntStorageFeatures(null,
            MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'maximal compression' with unsigned integers, that is deflation with the maximal
     * deflation level.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_UNSIGNED =
            new HDF5IntStorageFeatures(null, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_KEEP = new HDF5IntStorageFeatures(
            null, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'maximal compression' with unsigned integers, that is deflation with the maximal
     * deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(null, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR, false);

    /**
     * Represents 'maximal compression', that is deflation with the maximal deflation level.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_DELETE = new HDF5IntStorageFeatures(
            null, false, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR, true);

    /**
     * Represents 'maximal compression' with unsigned integers, that is deflation with the maximal
     * deflation level.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_DEFLATE_MAX_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(null, false, true, MAX_DEFLATION_LEVEL, NO_SCALING_FACTOR,
                    false);

    /**
     * Represents automatic scaling for integer values.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING = new HDF5IntStorageFeatures(null,
            NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR, true);

    /**
     * Represents automatic scaling for integer values with unsigned integers.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_UNSIGNED =
            new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR, false);

    /**
     * Represents automatic scaling for integer values.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_KEEP = new HDF5IntStorageFeatures(
            null, true, NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR, true);

    /**
     * Represents automatic scaling for integer values with unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(null, true, NO_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR,
                    false);

    /**
     * Represents automatic scaling for integer values.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DELETE =
            new HDF5IntStorageFeatures(null, false, true, NO_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, true);

    /**
     * Represents automatic scaling for integer values with unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(null, false, true, NO_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, false);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE =
            new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR,
                    true);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level, using unsigned integers.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_UNSIGNED =
            new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, INTEGER_AUTO_SCALING_FACTOR,
                    false);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_KEEP =
            new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, true);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level, using unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_UNSIGNED_KEEP =
            new HDF5IntStorageFeatures(null, true, DEFAULT_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, false);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_DELETE =
            new HDF5IntStorageFeatures(null, false, true, DEFAULT_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, true);

    /**
     * Represents automatic scaling for integer values combined with deflation with the default
     * deflation level, using unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static final HDF5IntStorageFeatures INT_AUTO_SCALING_DEFLATE_UNSIGNED_DELETE =
            new HDF5IntStorageFeatures(null, false, true, DEFAULT_DEFLATION_LEVEL,
                    INTEGER_AUTO_SCALING_FACTOR, false);

    /**
     * Create a corresponding {@link HDF5IntStorageFeatures} for the given
     * {@link HDF5GenericStorageFeatures}.
     */
    public static HDF5IntStorageFeatures createFromGeneric(
            HDF5GenericStorageFeatures storageFeatures)
    {
        if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED_DELETE)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED_KEEP)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT)
        {
            return HDF5IntStorageFeatures.INT_COMPACT;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT_DELETE)
        {
            return HDF5IntStorageFeatures.INT_COMPACT_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT_KEEP)
        {
            return HDF5IntStorageFeatures.INT_COMPACT_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS_DELETE)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS_KEEP)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION_DELETE)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION_KEEP)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_DELETE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_KEEP)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX_DELETE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX_KEEP)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX_KEEP;
        } else
        {
            return new HDF5IntStorageFeatures(storageFeatures.tryGetProposedLayout(),
                    storageFeatures.isKeepDataSetIfExists(),
                    storageFeatures.isDeleteDataSetIfExists(), storageFeatures.getDeflateLevel(),
                    NO_SCALING_FACTOR, true);
        }
    }

    /**
     * Create a corresponding {@link HDF5IntStorageFeatures} for the given
     * {@link HDF5GenericStorageFeatures}.
     */
    public static HDF5IntStorageFeatures createUnsignedFromGeneric(
            HDF5GenericStorageFeatures storageFeatures)
    {
        if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED_DELETE)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CHUNKED_KEEP)
        {
            return HDF5IntStorageFeatures.INT_CHUNKED_UNSIGNED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT)
        {
            return HDF5IntStorageFeatures.INT_COMPACT_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT_DELETE)
        {
            return HDF5IntStorageFeatures.INT_COMPACT_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_COMPACT_KEEP)
        {
            return HDF5IntStorageFeatures.INT_COMPACT_UNSIGNED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS_DELETE)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_CONTIGUOUS_KEEP)
        {
            return HDF5IntStorageFeatures.INT_CONTIGUOUS_UNSIGNED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION_DELETE)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION_KEEP)
        {
            return HDF5IntStorageFeatures.INT_NO_COMPRESSION_UNSIGNED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_DELETE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_KEEP)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_UNSIGNED_KEEP;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX_UNSIGNED;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX_DELETE)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX_UNSIGNED_DELETE;
        } else if (storageFeatures == HDF5GenericStorageFeatures.GENERIC_DEFLATE_MAX_KEEP)
        {
            return HDF5IntStorageFeatures.INT_DEFLATE_MAX_UNSIGNED_KEEP;
        } else
        {
            return new HDF5IntStorageFeatures(storageFeatures.tryGetProposedLayout(),
                    storageFeatures.isKeepDataSetIfExists(),
                    storageFeatures.isDeleteDataSetIfExists(), storageFeatures.getDeflateLevel(),
                    NO_SCALING_FACTOR, true);
        }
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5IntStorageFeatures createDeflation(int deflationLevel)
    {
        return createDeflation(deflationLevel, false, false, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createDeflationKeep(int deflationLevel)
    {
        return createDeflation(deflationLevel, true, false, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static HDF5IntStorageFeatures createDeflationDelete(int deflationLevel)
    {
        return createDeflation(deflationLevel, false, true, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var>.
     */
    public static HDF5IntStorageFeatures createDeflationUnsigned(int deflationLevel)
    {
        return createDeflation(deflationLevel, false, false, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var> for unsigned integers.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createDeflationUnsignedKeep(int deflationLevel)
    {
        return createDeflation(deflationLevel, true, false, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var> for unsigned integers.
     * <p>
     * Delete an existing data set before writing the new one. Always apply the chosen settings.
     * This allows to overwrite the {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()}
     * setting.
     */
    public static HDF5IntStorageFeatures createDeflationUnsignedDelete(int deflationLevel)
    {
        return createDeflation(deflationLevel, false, true, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflationLevel</var> for unsigned integers.
     */
    private static HDF5IntStorageFeatures createDeflation(int deflationLevel,
            boolean keepExistingDataSetIfExists, boolean deleteExistingDataSetIfExists,
            boolean signed)
    {
        if (signed)
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
                        deleteExistingDataSetIfExists, toByte(deflationLevel), NO_SCALING_FACTOR,
                        true);
            }
        } else
        {
            if (deflationLevel == NO_DEFLATION_LEVEL)
            {
                return keepExistingDataSetIfExists ? INT_NO_COMPRESSION_UNSIGNED_KEEP
                        : INT_NO_COMPRESSION_UNSIGNED;
            } else if (deflationLevel == DEFAULT_DEFLATION_LEVEL)
            {
                return keepExistingDataSetIfExists ? INT_DEFLATE_UNSIGNED_KEEP
                        : INT_DEFLATE_UNSIGNED;
            } else if (deflationLevel == MAX_DEFLATION_LEVEL)
            {
                return keepExistingDataSetIfExists ? INT_DEFLATE_MAX_UNSIGNED_KEEP
                        : INT_DEFLATE_MAX_UNSIGNED;
            } else
            {
                return new HDF5IntStorageFeatures(null, keepExistingDataSetIfExists,
                        deleteExistingDataSetIfExists, toByte(deflationLevel), NO_SCALING_FACTOR,
                        false);
            }
        }
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createIntegerScaling(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, false, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createIntegerScalingKeep(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, true, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createIntegerScalingUnsigned(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, false, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     * <p>
     * Keep existing data set and apply only if a new data set has to be created.
     */
    public static HDF5IntStorageFeatures createIntegerScalingUnsigedKeep(int scalingFactor)
    {
        return createIntegerScaling(scalingFactor, true, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents integer scaling with the
     * given <var>scalingFactor</var>.
     */
    private static HDF5IntStorageFeatures createIntegerScaling(int scalingFactor,
            boolean keepExistingDataSetIfExists, boolean signed)
    {
        if (signed)
        {
            if (scalingFactor == INTEGER_AUTO_SCALING_FACTOR)
            {
                return keepExistingDataSetIfExists ? INT_AUTO_SCALING_DEFLATE_KEEP
                        : INT_AUTO_SCALING_DEFLATE;
            } else
            {
                return new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, toByte(scalingFactor),
                        true);
            }
        } else
        {
            if (scalingFactor == INTEGER_AUTO_SCALING_FACTOR)
            {
                return keepExistingDataSetIfExists ? INT_AUTO_SCALING_DEFLATE_UNSIGNED_KEEP
                        : INT_AUTO_SCALING_DEFLATE_UNSIGNED;
            } else
            {
                return new HDF5IntStorageFeatures(null, NO_DEFLATION_LEVEL, toByte(scalingFactor),
                        false);
            }
        }
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the default
     * deflation level and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScaling(int scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, DEFAULT_DEFLATION_LEVEL, toByte(scalingFactor),
                true);
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
                toByte(scalingFactor), true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScaling(int deflateLevel,
            byte scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, toByte(deflateLevel), scalingFactor, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScalingUnsigned(int deflateLevel,
            byte scalingFactor)
    {
        return new HDF5IntStorageFeatures(null, toByte(deflateLevel), scalingFactor, false);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScaling(int deflateLevel,
            byte scalingFactor, boolean keepDataSetIfExists)
    {
        return new HDF5IntStorageFeatures(null, keepDataSetIfExists, toByte(deflateLevel),
                scalingFactor, true);
    }

    /**
     * Creates a {@link HDF5IntStorageFeatures} object that represents deflation with the given
     * <var>deflateLevel</var> and integer scaling with the given <var>scalingFactor</var>.
     */
    public static HDF5IntStorageFeatures createDeflateAndIntegerScalingUnsigned(int deflateLevel,
            byte scalingFactor, boolean keepDataSetIfExists)
    {
        return new HDF5IntStorageFeatures(null, keepDataSetIfExists, toByte(deflateLevel),
                scalingFactor, false);
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
        return new HDF5IntStorageFeatures(null, true, toByte(deflateLevel), scalingFactor, true);
    }

    private final boolean signed;

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, byte deflateLevel,
            byte scalingFactor, boolean signed)
    {
        this(proposedLayoutOrNull, false, deflateLevel, scalingFactor, signed);
    }

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, boolean keepDataSetIfExists,
            byte deflateLevel, byte scalingFactor, boolean signed)
    {
        super(proposedLayoutOrNull, keepDataSetIfExists, false, deflateLevel, scalingFactor);
        this.signed = signed;
    }

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, boolean keepDataSetIfExists,
            boolean deleteDataSetIfExists, byte deflateLevel, byte scalingFactor, boolean signed)
    {
        super(proposedLayoutOrNull, keepDataSetIfExists, deleteDataSetIfExists, deflateLevel,
                scalingFactor);
        this.signed = signed;
    }

    HDF5IntStorageFeatures(HDF5StorageLayout proposedLayoutOrNull, boolean keepDataSetIfExists,
            boolean deleteDataSetIfExists, boolean shuffleBeforeDeflate, byte deflateLevel,
            byte scalingFactor, boolean signed)
    {
        super(proposedLayoutOrNull, keepDataSetIfExists, deleteDataSetIfExists,
                shuffleBeforeDeflate, deflateLevel, scalingFactor);
        this.signed = signed;
    }

    /**
     * Returns <code>true</code> if signed integers should be stored, <code>false</code> otherwise.
     */
    public boolean isSigned()
    {
        return signed;
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
