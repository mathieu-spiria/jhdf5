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

import static ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5StringWriter}.
 * 
 * @author Bernd Rinn
 */
public class HDF5StringWriter implements IHDF5StringWriter
{

    private final HDF5BaseWriter baseWriter;

    HDF5StringWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setStringAttributeVariableLength(final String objectPath, final String name,
            final String value)
    {
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            setStringAttributeVariableLength(objectId, name, value, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private void setStringAttributeVariableLength(final int objectId, final String name,
            final String value, ICleanUpRegistry registry)
    {
        final int attributeId;
        if (baseWriter.h5.existsAttribute(objectId, name))
        {
            attributeId = baseWriter.h5.openAttribute(objectId, name, registry);
        } else
        {
            attributeId =
                    baseWriter.h5.createAttribute(objectId, name,
                            baseWriter.variableLengthStringDataTypeId, registry);
        }
        baseWriter.writeAttributeStringVL(attributeId, new String[]
            { value });
    }

    public void setStringAttribute(final String objectPath, final String name, final String value)
    {
        setStringAttribute(objectPath, name, value, value.length());
    }

    public void setStringAttribute(final String objectPath, final String name, final String value,
            final int maxLength)
    {
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            baseWriter.setStringAttribute(objectId, name, value, maxLength,
                                    registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeString(final String objectPath, final String data, final int maxLength)
    {
        writeString(objectPath, data, maxLength, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data)
    {
        writeString(objectPath, data, data.length(),
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data,
            final HDF5GenericStorageFeatures features)
    {
        writeString(objectPath, data, data.length(), features);
    }

    // Implementation note: this needs special treatment as we want to create a (possibly chunked)
    // data set with max dimension 1 instead of infinity.
    public void writeString(final String objectPath, final String data, final int maxLength,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int realMaxLength = maxLength + 1; // trailing '\0'
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(realMaxLength, registry);
                    final long[] chunkSizeOrNull =
                            HDF5Utils.tryGetChunkSizeForString(realMaxLength, features
                                    .requiresChunking());
                    final int dataSetId;
                    if (baseWriter.h5.exists(baseWriter.fileId, objectPath))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    } else
                    {
                        final HDF5StorageLayout layout =
                                baseWriter.determineLayout(stringDataTypeId,
                                        HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull, null);
                        dataSetId =
                                baseWriter.h5.createDataSet(baseWriter.fileId,
                                        HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull,
                                        stringDataTypeId, features, objectPath, layout,
                                        baseWriter.fileFormat, registry);
                    }
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            (data + '\0').getBytes());
                    return null; // Nothing to return.
                }

            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringArray(final String objectPath, final String[] data,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data), features);
    }

    public void writeStringArray(final String objectPath, final String[] data)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data),
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeStringArray(final String objectPath, final String[] data, final int maxLength)
    {
        writeStringArray(objectPath, data, maxLength,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    private static int getMaxLength(String[] data)
    {
        int maxLength = 0;
        for (String s : data)
        {
            maxLength = Math.max(maxLength, s.length());
        }
        return maxLength;
    }

    public void writeStringArray(final String objectPath, final String[] data, final int maxLength,
            final HDF5GenericStorageFeatures features) throws HDF5JavaException
    {
        assert objectPath != null;
        assert data != null;
        assert maxLength >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int realMaxLength = maxLength + 1; // Trailing '\0'
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(realMaxLength, registry);
                    final int dataSetId =
                            getDataSetIdForArray(objectPath, stringDataTypeId, data.length,
                                    realMaxLength, features, registry);
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                            maxLength);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    private int getDataSetIdForArray(final String objectPath, final int dataTypeId,
            final int arrayLength, final int elementLength,
            final HDF5GenericStorageFeatures features, ICleanUpRegistry registry)
    {
        int dataSetId;
        final long[] dimensions = new long[]
            { arrayLength };
        boolean exists = baseWriter.h5.exists(baseWriter.fileId, objectPath);
        if (exists && features.isKeepDataSetIfExists() == false)
        {
            baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
            exists = false;
        }
        if (exists)
        {
            dataSetId = baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
            final HDF5StorageLayout layout = baseWriter.h5.getLayout(dataSetId, registry);
            if (layout == HDF5StorageLayout.CHUNKED)
            {
                // Safety check. JHDF5 creates CHUNKED data sets always with unlimited
                // max dimensions but we may have to work on a file we haven't created.
                if (baseWriter.areDimensionsInBounds(dataSetId, dimensions))
                {
                    baseWriter.h5.setDataSetExtentChunked(dataSetId, dimensions);
                } else
                {
                    throw new HDF5JavaException("New data set dimension is out of bounds.");
                }
            } else
            {
                // CONTIGUOUS and COMPACT data sets are fixed size, thus we need to
                // delete and re-create it.
                baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
                dataSetId =
                        baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, null,
                                dataTypeId, GENERIC_NO_COMPRESSION, objectPath, layout,
                                baseWriter.fileFormat, registry);
            }
        } else
        {
            final long[] chunkSizeOrNull =
                    HDF5Utils.tryGetChunkSizeForStringVector(arrayLength, elementLength, features
                            .requiresChunking(), baseWriter.useExtentableDataTypes);
            final HDF5StorageLayout layout =
                    baseWriter.determineLayout(dataTypeId, dimensions, chunkSizeOrNull, features
                            .tryGetProposedLayout());
            dataSetId =
                    baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, chunkSizeOrNull,
                            dataTypeId, features, objectPath, layout, baseWriter.fileFormat,
                            registry);
        }
        return dataSetId;
    }

    public void createStringArray(final String objectPath, final int maxLength, final int size)
    {
        createStringArray(objectPath, maxLength, size, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize)
    {
        createStringArray(objectPath, maxLength, size, blockSize, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final int size,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert maxLength > 0;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(maxLength + 1, registry);
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert maxLength > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(maxLength + 1, registry);
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringArrayBlock(final String objectPath, final String[] data,
            final long blockNumber)
    {
        assert data != null;
        writeStringArrayBlockWithOffset(objectPath, data, data.length, data.length * blockNumber);
    }

    public void writeStringArrayBlockWithOffset(final String objectPath, final String[] data,
            final int dataSize, final long offset)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] blockDimensions = new long[]
                        { dataSize };
                    final long[] slabStartOrNull = new long[]
                        { offset };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { offset + dataSize }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    final int stringDataTypeId =
                            baseWriter.h5.getDataTypeForDataSet(dataSetId, registry);
                    if (baseWriter.h5.isVariableLengthString(stringDataTypeId))
                    {
                        baseWriter.writeStringVL(dataSetId, memorySpaceId, dataSpaceId, data);
                    } else
                    {
                        final int maxLength = baseWriter.h5.getDataTypeSize(stringDataTypeId) - 1;
                        H5Dwrite(dataSetId, stringDataTypeId, memorySpaceId, dataSpaceId,
                                H5P_DEFAULT, data, maxLength);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLength(final String objectPath, final String data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    baseWriter.writeScalarString(objectPath, data, -1);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLengthArray(final String objectPath, final String[] data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int pointerSize = 8; // 64bit pointers
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    final int dataSetId =
                            getDataSetIdForArray(objectPath, stringDataTypeId, data.length,
                                    pointerSize, GENERIC_NO_COMPRESSION, registry);
                    baseWriter.writeStringVL(dataSetId, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringVariableLengthArray(final String objectPath, final int size)
    {
        createStringVariableLengthArray(objectPath, size,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize) throws HDF5JavaException
    {
        createStringVariableLengthArray(objectPath, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringVariableLengthArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

}
