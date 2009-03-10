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

import static ch.systemsx.cisd.hdf5.HDF5Utils.BOOLEAN_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.getOneDimensionalArraySize;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.common.array.MDArray;
import ch.systemsx.cisd.common.process.CleanUpCallable;
import ch.systemsx.cisd.common.process.CleanUpRegistry;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;
import ch.systemsx.cisd.common.utilities.OSUtilities;

/**
 * If you want the reader to perform numeric conversions, call {@link #performNumericConversions()}
 * before calling {@link #reader()}.
 * 
 * @author Bernd Rinn
 */
public class HDF5BaseReader
{

    /** State that this reader / writer is currently in. */
    protected enum State
    {
        CONFIG, OPEN, CLOSED
    }

    protected final File hdf5File;

    protected final CleanUpCallable runner;

    protected final CleanUpRegistry fileRegistry;

    /** Map from named data types to ids. */
    final Map<String, Integer> namedDataTypeMap;

    protected HDF5 h5;

    protected int fileId;

    protected int booleanDataTypeId;

    protected HDF5EnumerationType typeVariantDataType;

    boolean performNumericConversions;

    protected State state;

    protected HDF5Reader readerWriterOrNull;

    public HDF5BaseReader(File hdf5File)
    {
        assert hdf5File != null;

        this.runner = new CleanUpCallable();
        this.fileRegistry = new CleanUpRegistry();
        this.namedDataTypeMap = new HashMap<String, Integer>();
        this.hdf5File = hdf5File.getAbsoluteFile();
    }

    protected void checkOpen() throws HDF5JavaException
    {
        if (state != State.OPEN)
        {
            final String msg =
                    "HDF5 file '" + hdf5File.getPath() + "' is "
                            + (state == State.CLOSED ? "closed." : "not opened yet.");
            throw new HDF5JavaException(msg);
        }
    }

    private void open()
    {
        final String path = hdf5File.getAbsolutePath();
        if (hdf5File.exists() == false)
        {
            throw new IllegalArgumentException("The file " + path + " does not exit.");
        }
        h5 = new HDF5(fileRegistry, performNumericConversions);
        fileId = h5.openFileReadOnly(path, fileRegistry);
        state = State.OPEN;
        readNamedDataTypes();
        booleanDataTypeId = openOrCreateBooleanDataType();
        typeVariantDataType = openOrCreateTypeVariantDataType();
    }

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    void close()
    {
        fileRegistry.cleanUp(false);
        state = State.CLOSED;
    }

    protected int openOrCreateBooleanDataType()
    {
        int dataTypeId = getDataTypeId(BOOLEAN_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = createBooleanDataType();
            commitDataType(BOOLEAN_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    protected int getDataTypeId(final String dataTypePath)
    {
        final Integer dataTypeIdOrNull = namedDataTypeMap.get(dataTypePath);
        if (dataTypeIdOrNull == null)
        {
            // Just in case of data types added to other groups than HDF5Utils.DATATYPE_GROUP
            if (readerWriterOrNull.exists(dataTypePath))
            {
                final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
                namedDataTypeMap.put(dataTypePath, dataTypeId);
                return dataTypeId;
            } else
            {
                return -1;
            }
        } else
        {
            return dataTypeIdOrNull;
        }
    }

    protected int createBooleanDataType()
    {
        return h5.createDataTypeEnum(new String[]
            { "FALSE", "TRUE" }, fileRegistry);
    }

    protected HDF5EnumerationType openOrCreateTypeVariantDataType()
    {
        int dataTypeId = getDataTypeId(TYPE_VARIANT_DATA_TYPE);
        if (dataTypeId < 0)
        {
            return createTypeVariantDataType();
        }
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    protected HDF5EnumerationType createTypeVariantDataType()
    {
        final HDF5DataTypeVariant[] typeVariants = HDF5DataTypeVariant.values();
        final String[] typeVariantNames = new String[typeVariants.length];
        for (int i = 0; i < typeVariants.length; ++i)
        {
            typeVariantNames[i] = typeVariants[i].name();
        }
        final int dataTypeId = h5.createDataTypeEnum(typeVariantNames, fileRegistry);
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    protected void readNamedDataTypes()
    {
        if (readerWriterOrNull.exists(DATATYPE_GROUP) == false)
        {
            return;
        }
        for (String dataTypePath : readerWriterOrNull.getGroupMemberPaths(DATATYPE_GROUP))
        {
            final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
            namedDataTypeMap.put(dataTypePath, dataTypeId);
        }
    }

    protected void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        // Overwrite method in writer.
    }

    /**
     * Class to store the parameters of a 1d data space.
     */
    static class DataSpaceParameters
    {
        final int memorySpaceId;

        final int dataSpaceId;

        final int blockSize;

        final long[] dimensions;

        DataSpaceParameters(int memorySpaceId, int dataSpaceId, int blockSize, long[] dimensions)
        {
            this.memorySpaceId = memorySpaceId;
            this.dataSpaceId = dataSpaceId;
            this.blockSize = blockSize;
            this.dimensions = dimensions;
        }
    }

    /**
     * Returns the {@link DataSpaceParameters} for the given <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, ICleanUpRegistry registry)
    {
        long[] dimensions = h5.getDataDimensions(dataSetId);
        // Ensure backward compatibility with 8.10
        if (HDF5Utils.mightBeEmptyInStorage(dimensions)
                && h5.existsAttribute(dataSetId, HDF5Utils.DATASET_IS_EMPTY_LEGACY_ATTRIBUTE))
        {
            dimensions = new long[dimensions.length];
        }
        return new DataSpaceParameters(H5S_ALL, H5S_ALL, MDArray.getLength(dimensions), dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a 1d block of the given <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, final long offset,
            final int blockSize, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final int actualBlockSize;
        final long[] dimensions;
        if (blockSize > 0)
        {
            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            dimensions = h5.getDataSpaceDimensions(dataSpaceId);
            if (dimensions.length != 1)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                        + dimensions.length + ")");
            }
            final long size = dimensions[0];
            final long maxBlockSize = size - offset;
            if (maxBlockSize <= 0)
            {
                throw new HDF5JavaException("Offset " + offset + " >= Size " + size);
            }
            actualBlockSize = (int) Math.min(blockSize, maxBlockSize);
            final long[] blockShape = new long[]
                { actualBlockSize };
            h5.setHyperslabBlock(dataSpaceId, new long[]
                { offset }, blockShape);
            memorySpaceId = h5.createSimpleDataSpace(blockShape, registry);

        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            dimensions = h5.getDataDimensions(dataSetId);
            actualBlockSize = getOneDimensionalArraySize(dimensions);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, actualBlockSize, dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a multi-dimensional block of the given
     * <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, final long[] offset,
            final int[] blockDimensionsOrNull, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final long[] effectiveBlockDimensions;
        if (blockDimensionsOrNull != null)
        {
            assert offset != null;
            assert blockDimensionsOrNull.length == offset.length;

            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            final long[] dimensions = h5.getDataSpaceDimensions(dataSpaceId);
            if (dimensions.length != blockDimensionsOrNull.length)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank "
                        + blockDimensionsOrNull.length + " (rank=" + dimensions.length + ")");
            }
            effectiveBlockDimensions = new long[blockDimensionsOrNull.length];
            for (int i = 0; i < offset.length; ++i)
            {
                final long maxBlockSize = dimensions[i] - offset[i];
                if (maxBlockSize <= 0)
                {
                    throw new HDF5JavaException("Offset " + offset[i] + " >= Size " + dimensions[i]);
                }
                effectiveBlockDimensions[i] = Math.min(blockDimensionsOrNull[i], maxBlockSize);
            }
            h5.setHyperslabBlock(dataSpaceId, offset, effectiveBlockDimensions);
            memorySpaceId = h5.createSimpleDataSpace(effectiveBlockDimensions, registry);
        } else
        {
            memorySpaceId = H5S_ALL;
            dataSpaceId = H5S_ALL;
            effectiveBlockDimensions = h5.getDataDimensions(dataSetId);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, MDArray
                .getLength(effectiveBlockDimensions), effectiveBlockDimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for the given <var>dataSetId</var> when they are
     * mapped to a block in memory.
     */
    DataSpaceParameters getBlockSpaceParameters(final int dataSetId, final int[] memoryOffset,
            final int[] memoryDimensions, ICleanUpRegistry registry)
    {
        final long[] dimensions = h5.getDataDimensions(dataSetId);
        final int memorySpaceId =
                h5.createSimpleDataSpace(MDArray.toLong(memoryDimensions), registry);
        h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset), dimensions);
        return new DataSpaceParameters(memorySpaceId, H5S_ALL, MDArray.getLength(dimensions),
                dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a block of the given <var>dataSetId</var> when
     * they are mapped to a block in memory.
     */
    DataSpaceParameters getBlockSpaceParameters(final int dataSetId, final int[] memoryOffset,
            final int[] memoryDimensions, final long[] offset, final int[] blockDimensions,
            ICleanUpRegistry registry)
    {
        assert memoryOffset != null;
        assert memoryDimensions != null;
        assert offset != null;
        assert blockDimensions != null;
        assert memoryOffset.length == offset.length;
        assert memoryDimensions.length == memoryOffset.length;
        assert blockDimensions.length == offset.length;

        final int memorySpaceId;
        final int dataSpaceId;
        final long[] effectiveBlockDimensions;

        dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
        final long[] dimensions = h5.getDataSpaceDimensions(dataSpaceId);
        if (dimensions.length != blockDimensions.length)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank "
                    + blockDimensions.length + " (rank=" + dimensions.length + ")");
        }
        effectiveBlockDimensions = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            final long maxBlockSize = dimensions[i] - offset[i];
            if (maxBlockSize <= 0)
            {
                throw new HDF5JavaException("Offset " + offset[i] + " >= Size " + dimensions[i]);
            }
            effectiveBlockDimensions[i] = Math.min(blockDimensions[i], maxBlockSize);
        }
        h5.setHyperslabBlock(dataSpaceId, offset, effectiveBlockDimensions);
        memorySpaceId = h5.createSimpleDataSpace(MDArray.toLong(memoryDimensions), registry);
        h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset), effectiveBlockDimensions);
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, MDArray
                .getLength(effectiveBlockDimensions), effectiveBlockDimensions);
    }

    /**
     * Returns the native data type for the given <var>dataSetId</var>, or
     * <var>overrideDataTypeId</var>, if it is not negative.
     */
    int getNativeDataTypeId(final int dataSetId, final int overrideDataTypeId,
            ICleanUpRegistry registry)
    {
        final int nativeDataTypeId;
        if (overrideDataTypeId < 0)
        {
            nativeDataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
        } else
        {
            nativeDataTypeId = overrideDataTypeId;
        }
        return nativeDataTypeId;
    }

    //
    // HDF5ReaderConfig
    //

    /**
     * Returns <code>true</code>, if this platform supports numeric conversions.
     */
    public boolean platformSupportsNumericConversions()
    {
        // On HDF5 1.8.2, numeric conversions on sparcv9 can get us SEGFAULTS for converting between
        // integers and floats.
        if (OSUtilities.getCPUArchitecture().startsWith("sparc"))
        {
            return false;
        }
        return true;
    }

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    public HDF5BaseReader performNumericConversions()
    {
        if (platformSupportsNumericConversions() == false)
        {
            return this;
        }
        this.performNumericConversions = true;
        return this;
    }

    /**
     * Returns an {@link HDF5Reader} based on this configuration.
     */
    public HDF5Reader reader()
    {
        if (readerWriterOrNull == null)
        {
            readerWriterOrNull = new HDF5Reader(this);
            open();
        }
        return readerWriterOrNull;
    }

}
