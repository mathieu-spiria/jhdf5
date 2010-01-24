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

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

/**
 * An object representing the storage features that are to be used for a data set.
 * <p>
 * The available storage layouts are {@link HDF5StorageLayout#COMPACT},
 * {@link HDF5StorageLayout#CONTIGUOUS} or {@link HDF5StorageLayout#CHUNKED} can be chosen. Only
 * {@link HDF5StorageLayout#CHUNKED} is extendable and can be compressed.
 * <p>
 * Two types of compressions are supported: <i>deflation</i> (the method used by <code>gzip</code>)
 * and <i>scaling</i>, which can be used if the accuracy of the values are smaller than what the
 * atomic data type can store. Note that <i>scaling</i> in general can be a lossy compression while
 * <i>deflation</i> is always lossless. <i>Scaling</i> compression is only available with HDF5 1.8
 * and newer. Trying to use <i>scaling</i> in strict HDF5 1.6 compatibility mode will throw an
 * {@link IllegalStateException}.
 * <p>
 * For <i>deflation</i> the deflation level can be chosen to get the right balance between speed of
 * compression and compression ratio. Often the {@link #DEFAULT_DEFLATION_LEVEL} will be the right
 * choice.
 * <p>
 * For <i>scaling</i>, the scaling factor can be chosen that determines the accuracy of the values
 * saved. What exactly the scaling factor means, differs between float and integer values.
 * 
 * @author Bernd Rinn
 */
abstract class HDF5AbstractStorageFeatures
{
    /**
     * A constant that specifies that no deflation should be used.
     */
    public final static byte NO_DEFLATION_LEVEL = 0;

    /**
     * A constant that specifies the default deflation level (gzip compression).
     */
    public final static byte DEFAULT_DEFLATION_LEVEL = 6;

    /**
     * A constant that specifies the maximal deflation level (gzip compression).
     */
    public final static byte MAX_DEFLATION_LEVEL = 9;

    /**
     * Do not perform any scaling on the data.
     */
    final static byte NO_SCALING_FACTOR = -1;

    static byte toByte(int i)
    {
        final byte b = (byte) i;
        if (b != i)
        {
            throw new HDF5JavaException("Value " + i + " cannot be casted to type byte");
        }
        return b;
    }

    private final byte deflateLevel;

    private final byte scalingFactor;

    private final HDF5StorageLayout proposedLayoutOrNull;
    
    private final boolean keepDataSetIfExists;

    HDF5AbstractStorageFeatures(final HDF5StorageLayout proposedLayoutOrNull,
            final boolean keepDataSetIfExists, final byte deflateLevel, final byte scalingFactor)
    {
        assert deflateLevel >= 0;

        this.proposedLayoutOrNull = proposedLayoutOrNull;
        this.keepDataSetIfExists = keepDataSetIfExists;
        this.deflateLevel = deflateLevel;
        this.scalingFactor = scalingFactor;
    }

    /**
     * Returns true, if this compression setting can be applied on the given <var>dataClassId</var>.
     */
    abstract boolean isCompatibleWithDataClass(int dataClassId);

    /**
     * Returns the proposed storage layout, or <code>null</code>, if no particular storage layout
     * should be proposed.
     */
    public HDF5StorageLayout tryGetProposedLayout()
    {
        return proposedLayoutOrNull;
    }

    boolean requiresChunking()
    {
        return isDeflating() || isScaling() || proposedLayoutOrNull == HDF5StorageLayout.CHUNKED;
    }

    public boolean isDeflating()
    {
        return (deflateLevel != NO_DEFLATION_LEVEL);
    }

    public boolean isScaling()
    {
        return scalingFactor >= 0;
    }

    boolean isKeepDataSetIfExists()
    {
        return keepDataSetIfExists;
    }

    void checkScalingOK(FileFormat fileFormat) throws IllegalStateException
    {
        if (fileFormat.isHDF5_1_8_OK() == false)
        {
            throw new IllegalStateException(
                    "Scaling compression is not allowed in strict HDF5 1.6.x compatibility mode.");
        }
    }

    byte getDeflateLevel()
    {
        return deflateLevel;
    }

    byte getScalingFactor()
    {
        return scalingFactor;
    }

}
