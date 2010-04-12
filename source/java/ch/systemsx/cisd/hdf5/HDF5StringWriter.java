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
import static ncsa.hdf.hdf5lib.H5.H5DwriteString;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_SCALAR;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
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
                            baseWriter.setStringAttributeVariableLength(objectId, name, value,
                                    registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
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

    public void setStringArrayAttribute(final String objectPath, final String name,
            final String[] value, final int maxLength)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> setAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int objectId =
                        baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                registry);
                    baseWriter.setStringArrayAttribute(objectId, name, value, maxLength, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    public void setStringArrayAttribute(final String objectPath, final String name,
            final String[] value)
    {
        setStringArrayAttribute(objectPath, name, value, getMaxLength(value));
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
        assert maxLength >= 0;

        writeStringArray(objectPath, data, maxLength, features, false);
    }

    private void writeStringArray(final String objectPath, final String[] data,
            final int maxLength, final HDF5GenericStorageFeatures features,
            final boolean variableLength)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int elementSize;
                    final int stringDataTypeId;
                    if (variableLength)
                    {
                        elementSize = 8; // 64bit pointers
                        stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    } else
                    {
                        elementSize = maxLength + 1; // Trailing '\0'
                        stringDataTypeId =
                                baseWriter.h5.createDataTypeString(elementSize, registry);
                    }
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, stringDataTypeId, new long[]
                                { data.length }, elementSize, features, registry);
                    if (variableLength)
                    {
                        baseWriter.writeStringVL(dataSetId, data);
                    } else
                    {
                        H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                                maxLength);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
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
        assert maxLength > 0;

        createStringArray(objectPath, maxLength, size, features, false);
    }

    private void createStringArray(final String objectPath, final int maxLength, final int size,
            final HDF5GenericStorageFeatures features, final boolean variableLength)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int elementSize;
                    final int stringDataTypeId;
                    if (variableLength)
                    {
                        elementSize = 8; // 64bit pointers
                        stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    } else
                    {
                        elementSize = maxLength + 1;
                        stringDataTypeId =
                                baseWriter.h5.createDataTypeString(elementSize, registry);
                    }
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, elementSize, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { size }, null, elementSize, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert maxLength > 0;

        createStringArray(objectPath, maxLength, size, blockSize, features, false);
    }

    private void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features,
            final boolean variableLength)
    {
        assert objectPath != null;
        assert blockSize > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int elementSize;
                    final int stringDataTypeId;
                    if (variableLength)
                    {
                        elementSize = 8; // 64bit pointers
                        stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    } else
                    {
                        elementSize = maxLength + 1;
                        stringDataTypeId =
                                baseWriter.h5.createDataTypeString(elementSize, registry);
                    }
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, elementSize, registry);
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

    public void writeStringMDArray(final String objectPath, final MDArray<String> data)
            throws HDF5JavaException
    {
        writeStringMDArray(objectPath, data, getMaxLength(data.getAsFlatArray()),
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeStringMDArray(final String objectPath, final MDArray<String> data,
            final int maxLength) throws HDF5JavaException
    {
        writeStringMDArray(objectPath, data, maxLength,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeStringMDArray(final String objectPath, final MDArray<String> data,
            final int maxLength, final HDF5GenericStorageFeatures features)
            throws HDF5JavaException
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
                            baseWriter.getDataSetId(objectPath, stringDataTypeId, data
                                    .longDimensions(), realMaxLength, features, registry);
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data
                            .getAsFlatArray(), maxLength);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringMDArray(final String objectPath, final int maxLength,
            final int[] dimensions)
    {
        createStringMDArray(objectPath, maxLength, dimensions, GENERIC_NO_COMPRESSION);
    }

    public void createStringMDArray(final String objectPath, final int maxLength,
            final long[] dimensions, final int[] blockSize)
    {
        createStringMDArray(objectPath, maxLength, dimensions, blockSize, GENERIC_NO_COMPRESSION);
    }

    public void createStringMDArray(final String objectPath, final int maxLength,
            final int[] dimensions, final HDF5GenericStorageFeatures features)
    {
        assert maxLength > 0;

        createStringMDArray(objectPath, maxLength, dimensions, features, false);
    }

    private void createStringMDArray(final String objectPath, final int maxLength,
            final int[] dimensions, final HDF5GenericStorageFeatures features,
            final boolean variableLength)
    {
        assert objectPath != null;
        assert dimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int elementSize;
                    final int stringDataTypeId;
                    if (variableLength)
                    {
                        elementSize = 8; // 64bit pointers
                        stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    } else
                    {
                        elementSize = maxLength + 1;
                        stringDataTypeId =
                                baseWriter.h5.createDataTypeString(elementSize, registry);
                    }
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, MDArray.toLong(dimensions), maxLength, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, MDArray
                                .toLong(dimensions), null, maxLength, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringMDArray(final String objectPath, final int maxLength,
            final long[] dimensions, final int[] blockSize,
            final HDF5GenericStorageFeatures features)
    {
        assert maxLength > 0;

        createStringMDArray(objectPath, maxLength, dimensions, blockSize, features, false);
    }

    private void createStringMDArray(final String objectPath, final int maxLength,
            final long[] dimensions, final int[] blockSize,
            final HDF5GenericStorageFeatures features, final boolean variableLength)
    {
        assert objectPath != null;
        assert dimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int elementSize;
                    final int stringDataTypeId;
                    if (variableLength)
                    {
                        elementSize = 8; // 64bit pointers
                        stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    } else
                    {
                        elementSize = maxLength + 1;
                        stringDataTypeId =
                                baseWriter.h5.createDataTypeString(elementSize, registry);
                    }
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, dimensions,
                            MDArray.toLong(blockSize), elementSize, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringMDArrayBlock(final String objectPath, final MDArray<String> data,
            final long[] blockNumber)
    {
        assert data != null;
        assert blockNumber != null;

        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
        }
        writeStringMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeStringMDArrayBlockWithOffset(final String objectPath,
            final MDArray<String> data, final long[] offset)
    {
        assert objectPath != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = data.longDimensions();
                    assert dimensions.length == offset.length;
                    final long[] dataSetDimensions = new long[dimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + dimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final int stringDataTypeId =
                            baseWriter.h5.getDataTypeForDataSet(dataSetId, registry);
                    if (baseWriter.h5.isVariableLengthString(stringDataTypeId))
                    {
                        baseWriter.writeStringVL(dataSetId, memorySpaceId, dataSpaceId, data
                                .getAsFlatArray());
                    } else
                    {
                        final int maxLength = baseWriter.h5.getDataTypeSize(stringDataTypeId) - 1;
                        H5Dwrite(dataSetId, stringDataTypeId, memorySpaceId, dataSpaceId,
                                H5P_DEFAULT, data.getAsFlatArray(), maxLength);
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
                    final int dataSetId;
                    if (baseWriter.h5.exists(baseWriter.fileId, objectPath))
                    {
                        dataSetId =
                                baseWriter.h5.openObject(baseWriter.fileId, objectPath, registry);
                    } else
                    {
                        dataSetId =
                                baseWriter.h5.createScalarDataSet(baseWriter.fileId,
                                        baseWriter.variableLengthStringDataTypeId, objectPath,
                                        registry);
                    }
                    H5DwriteString(dataSetId, baseWriter.variableLengthStringDataTypeId,
                            H5S_SCALAR, H5S_SCALAR, H5P_DEFAULT, new String[]
                                { data });
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLengthArray(final String objectPath, final String[] data)
    {
        writeStringVariableLengthArray(objectPath, data, GENERIC_NO_COMPRESSION);
    }

    public void writeStringVariableLengthArray(final String objectPath, final String[] data,
            final HDF5GenericStorageFeatures features)
    {
        writeStringArray(objectPath, data, -1, features, true);
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
        createStringArray(objectPath, -1, size, blockSize, features, true);
    }

    public void createStringVariableLengthArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features)
    {
        createStringArray(objectPath, -1, size, features, true);
    }

    public void createStringVariableLengthMDArray(final String objectPath, final int[] dimensions,
            final HDF5GenericStorageFeatures features)
    {
        createStringMDArray(objectPath, -1, dimensions, features, true);
    }

    public void createStringVariableLengthMDArray(final String objectPath, final int[] dimensions)
    {
        createStringMDArray(objectPath, -1, dimensions, GENERIC_NO_COMPRESSION, true);
    }

    public void createStringVariableLengthMDArray(final String objectPath, final long[] dimensions,
            final int[] blockSize, final HDF5GenericStorageFeatures features)
    {
        createStringMDArray(objectPath, -1, dimensions, blockSize, features, true);
    }

    public void createStringVariableLengthMDArray(final String objectPath, final long[] dimensions,
            final int[] blockSize)
    {
        createStringMDArray(objectPath, -1, dimensions, blockSize, GENERIC_NO_COMPRESSION, true);
    }

    public void writeStringVariableLengthMDArray(final String objectPath,
            final MDArray<String> data, final HDF5GenericStorageFeatures features)
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
                            baseWriter.getDataSetId(objectPath, stringDataTypeId, MDArray
                                    .toLong(data.dimensions()), pointerSize, features, registry);
                    baseWriter.writeStringVL(dataSetId, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLengthMDArray(final String objectPath, final MDArray<String> data)
    {
        writeStringVariableLengthMDArray(objectPath, data, GENERIC_NO_COMPRESSION);
    }
}
