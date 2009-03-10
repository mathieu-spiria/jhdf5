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

import static ch.systemsx.cisd.hdf5.HDF5.NO_DEFLATION;
import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.isEmpty;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_SCALAR;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_UNLIMITED;

import java.io.File;

import ch.systemsx.cisd.common.process.ICallableWithCleanUp;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * The configuration of this writer config is done by chaining calls to methods {@link #overwrite()}
 * , {@link #dontUseExtendableDataTypes()} and {@link #useLatestFileFormat()} before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
public final class HDF5BaseWriter extends HDF5BaseReader
{

    /**
     * The size threshold for the COMPACT storage layout.
     */
    final static int COMPACT_LAYOUT_THRESHOLD = 256;

    boolean useExtentableDataTypes = true;

    boolean overwrite = false;

    boolean useLatestFileFormat = false;

    int variableLengthStringDataTypeId;

    public HDF5BaseWriter(File hdf5File)
    {
        super(hdf5File);
    }

    @Override
    protected void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        h5.commitDataType(fileId, dataTypePath, dataTypeId);
    }

    private void open()
    {
        final String path = hdf5File.getAbsolutePath();
        h5 = new HDF5(fileRegistry, true);
        fileId = openOrCreateFile(path);
        state = State.OPEN;
        readNamedDataTypes();
        booleanDataTypeId = openOrCreateBooleanDataType();
        typeVariantDataType = openOrCreateTypeVariantDataType();
        variableLengthStringDataTypeId = openOrCreateVLStringType();
    }

    private int openOrCreateFile(final String path)
    {
        if (hdf5File.exists() && overwrite == false)
        {
            return h5.openFileReadWrite(path, useLatestFileFormat, fileRegistry);
        } else
        {
            final File directory = hdf5File.getParentFile();
            if (directory.exists() == false)
            {
                throw new HDF5JavaException("Directory '" + directory.getPath()
                        + "' does not exist.");
            }
            return h5.createFile(path, useLatestFileFormat, fileRegistry);
        }
    }

    protected HDF5EnumerationType openOrCreateTypeVariantDataType(final HDF5Writer writer)
    {
        final HDF5EnumerationType dataType;
        int dataTypeId = getDataTypeId(HDF5Utils.TYPE_VARIANT_DATA_TYPE);
        if (dataTypeId < 0
                || h5.getNumberOfMembers(dataTypeId) < HDF5DataTypeVariant.values().length)
        {
            final String typeVariantPath = findFirstUnusedTypeVariantPath(writer);
            dataType = createTypeVariantDataType();
            commitDataType(typeVariantPath, dataType.getStorageTypeId());
            writer.createOrUpdateSoftLink(typeVariantPath.substring(DATATYPE_GROUP.length() + 1),
                    TYPE_VARIANT_DATA_TYPE);
        } else
        {
            final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
            final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
            dataType =
                    new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                            TYPE_VARIANT_DATA_TYPE, typeVariantNames);

        }
        return dataType;
    }

    private final static int MAX_TYPE_VARIANT_TYPES = 1024;

    private String findFirstUnusedTypeVariantPath(final HDF5Reader reader)
    {
        int number = 0;
        String path;
        do
        {
            path = TYPE_VARIANT_DATA_TYPE + "." + (number++);
        } while (reader.exists(path) && number < MAX_TYPE_VARIANT_TYPES);
        return path;
    }

    private int openOrCreateVLStringType()
    {
        int dataTypeId = getDataTypeId(HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = h5.createDataTypeVariableString(fileRegistry);
            commitDataType(VARIABLE_LENGTH_STRING_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    /**
     * Write a scalar value provided as <code>byte[]</code>.
     */
    void writeScalar(final String dataSetPath, final int storageDataTypeId,
            final int nativeDataTypeId, final byte[] value)
    {
        assert dataSetPath != null;
        assert storageDataTypeId >= 0;
        assert nativeDataTypeId >= 0;
        assert value != null;

        final ICallableWithCleanUp<Object> writeScalarRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    writeScalar(dataSetPath, storageDataTypeId, nativeDataTypeId, value, registry);
                    return null; // Nothing to return.
                }
            };
        runner.call(writeScalarRunnable);
    }

    /**
     * Internal method for writing a scalar value provided as <code>byte[]</code>.
     */
    int writeScalar(final String dataSetPath, final int storageDataTypeId,
            final int nativeDataTypeId, final byte[] value, ICleanUpRegistry registry)
    {
        final int dataSetId;
        if (h5.exists(fileId, dataSetPath))
        {
            dataSetId = h5.openObject(fileId, dataSetPath, registry);
        } else
        {
            dataSetId = h5.createScalarDataSet(fileId, storageDataTypeId, dataSetPath, registry);
        }
        H5Dwrite(dataSetId, nativeDataTypeId, H5S_SCALAR, H5S_SCALAR, H5P_DEFAULT, value);
        return dataSetId;
    }

    /**
     * Creates a data set.
     */
    int createDataSet(final String objectPath, final int storageDataTypeId, final int deflateLevel,
            final long[] dimensions, final long[] chunkSizeOrNull, boolean enforceCompactLayout,
            ICleanUpRegistry registry)
    {
        final int dataSetId;
        final boolean deflate = (deflateLevel != NO_DEFLATION);
        final boolean empty = isEmpty(dimensions);
        final long[] definitiveChunkSizeOrNull;
        if (empty)
        {
            definitiveChunkSizeOrNull = HDF5Utils.tryGetChunkSize(dimensions, deflate, true);
        } else if (enforceCompactLayout)
        {
            definitiveChunkSizeOrNull = null;
        } else if (chunkSizeOrNull != null)
        {
            definitiveChunkSizeOrNull = chunkSizeOrNull;
        } else
        {
            definitiveChunkSizeOrNull =
                    HDF5Utils.tryGetChunkSize(dimensions, deflate, useExtentableDataTypes);
        }
        final StorageLayout layout =
                determineLayout(storageDataTypeId, dimensions, definitiveChunkSizeOrNull,
                        enforceCompactLayout);
        dataSetId =
                h5.createDataSet(fileId, dimensions, definitiveChunkSizeOrNull, storageDataTypeId,
                        deflateLevel, objectPath, layout, registry);
        return dataSetId;
    }

    /**
     * Determine which {@link StorageLayout} to use for the given <var>storageDataTypeId</var>.
     */
    StorageLayout determineLayout(final int storageDataTypeId, final long[] dimensions,
            final long[] chunkSizeOrNull, boolean enforceCompactLayout)
    {
        if (chunkSizeOrNull != null)
        {
            return StorageLayout.CHUNKED;
        }
        if (enforceCompactLayout
                || computeSizeForDimensions(storageDataTypeId, dimensions) < HDF5BaseWriter.COMPACT_LAYOUT_THRESHOLD)
        {
            return StorageLayout.COMPACT;
        }
        return StorageLayout.CONTIGUOUS;
    }

    private int computeSizeForDimensions(int dataTypeId, long[] dimensions)
    {
        int size = h5.getSize(dataTypeId);
        for (long d : dimensions)
        {
            size *= d;
        }
        return size;
    }

    /**
     * Checks whether the given <var>dimensions</var> are in bounds for <var>dataSetId</var>.
     */
    boolean areDimensionsInBounds(final int dataSetId, final long[] dimensions)
    {
        final long[] maxDimensions = h5.getDataMaxDimensions(dataSetId);

        if (dimensions.length != maxDimensions.length) // Actually an error condition
        {
            return false;
        }

        for (int i = 0; i < dimensions.length; ++i)
        {
            if (maxDimensions[i] != H5S_UNLIMITED && dimensions[i] > maxDimensions[i])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the data set id for the given <var>objectPath</var>.
     */
    int getDataSetId(final String objectPath, final int storageDataTypeId, long[] dimensions,
            final int deflateLevel, ICleanUpRegistry registry)
    {
        final int dataSetId;
        if (h5.exists(fileId, objectPath))
        {
            dataSetId = h5.openDataSet(fileId, objectPath, registry);
            // Implementation note: HDF5 1.8 seems to be able to change the size even if
            // dimensions are not in bound of max dimensions, but the resulting file can
            // no longer be read by HDF5 1.6, thus we may only do it if config.useLatestFileFormat
            // == true.
            if (areDimensionsInBounds(dataSetId, dimensions) || useLatestFileFormat)
            {
                h5.setDataSetExtent(dataSetId, dimensions);
                // FIXME 2008-09-15, Bernd Rinn: This is a work-around for an apparent bug in HDF5
                // 1.8.1 and 1.8.2 with contiguous data sets! Without the flush, the next
                // config.h5.writeDataSet() call will not overwrite the data.
                if (h5.getLayout(dataSetId, registry) == StorageLayout.CONTIGUOUS)
                {
                    h5.flushFile(fileId);
                }
            }
        } else
        {
            dataSetId =
                    createDataSet(objectPath, storageDataTypeId, deflateLevel, dimensions, null,
                            false, registry);
        }
        return dataSetId;
    }

    //
    // Config
    //

    /**
     * The file will be truncated to length 0 if it already exists, that is its content will be
     * deleted.
     */
    public HDF5BaseWriter overwrite()
    {
        this.overwrite = true;
        return this;
    }

    /**
     * Use data types which can not be extended later on. This may reduce the initial size of the
     * HDF5 file.
     */
    public HDF5BaseWriter dontUseExtendableDataTypes()
    {
        this.useExtentableDataTypes = false;
        return this;
    }

    /**
     * A file will be created that uses the latest available file format. This may improve
     * performance or space consumption but in general means that older versions of the library are
     * no longer able to read this file.
     */
    public HDF5BaseWriter useLatestFileFormat()
    {
        this.useLatestFileFormat = true;
        return this;
    }

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    @Override
    public HDF5BaseWriter performNumericConversions()
    {
        return (HDF5BaseWriter) super.performNumericConversions();
    }

    /**
     * Returns an {@link HDF5Writer} based on this configuration.
     */
    public HDF5Writer writer()
    {
        if (readerWriterOrNull == null)
        {
            readerWriterOrNull = new HDF5Writer(this);
            open();
        }
        return (HDF5Writer) readerWriterOrNull;
    }

}
