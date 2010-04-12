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

import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5CompoundWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundWriter extends HDF5CompoundInformationRetriever implements IHDF5CompoundWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5CompoundWriter(HDF5BaseWriter baseWriter)
    {
        super(baseWriter);
        this.baseWriter = baseWriter;
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members)
    {
        baseWriter.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectByteifyer =
                baseWriter.createCompoundByteifyers(pojoClass, members);
        final String dataTypeName = (name != null) ? name : pojoClass.getSimpleName();
        final int storageDataTypeId =
                getOrCreateCompoundDataType(dataTypeName, pojoClass, objectByteifyer);
        final int nativeDataTypeId = baseWriter.createNativeCompoundDataType(objectByteifyer);
        return new HDF5CompoundType<T>(baseWriter.fileId, storageDataTypeId, nativeDataTypeId,
                dataTypeName, pojoClass, objectByteifyer);
    }

    private <T> int getOrCreateCompoundDataType(final String dataTypeName,
            final Class<T> compoundClass, final HDF5ValueObjectByteifyer<T> objectByteifyer)
    {
        final String dataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName);
        int storageDataTypeId = baseWriter.getDataTypeId(dataTypePath);
        if (storageDataTypeId < 0)
        {
            storageDataTypeId = baseWriter.createStorageCompoundDataType(objectByteifyer);
            baseWriter.commitDataType(dataTypePath, storageDataTypeId);
        }
        return storageDataTypeId;
    }

    public <T> void writeCompound(final String objectPath, final HDF5CompoundType<T> type,
            final T data)
    {
        writeCompound(objectPath, type, data, null);
    }

    public <T> void writeCompound(final String objectPath, final HDF5CompoundType<T> type,
            final T data, final IByteArrayInspector inspectorOrNull)
    {
        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final byte[] byteArray = type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
        if (inspectorOrNull != null)
        {
            inspectorOrNull.inspect(byteArray);
        }
        baseWriter.writeScalar(objectPath, type.getStorageTypeId(), type.getNativeTypeId(),
                byteArray);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data)
    {
        writeCompoundArray(objectPath, type, data,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures features)
    {
        writeCompoundArray(objectPath, type, data, features, null);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures features,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, type.getStorageTypeId(), new long[]
                                { data.length }, type.getObjectByteifyer().getRecordSize(),
                                    features, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber)
    {
        writeCompoundArrayBlock(objectPath, type, data, blockNumber, null);
    }

    public <T> void writeCompoundArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert blockNumber >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long size = data.length;
                    final long[] dimensions = new long[]
                        { size };
                    final long[] offset = new long[]
                        { size * blockNumber };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { data.length * (blockNumber + 1) }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset)
    {
        writeCompoundArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final long size = data.length;
        final long[] dimensions = new long[]
            { size };
        final long[] offsetArray = new long[]
            { offset };
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { offset + data.length }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offsetArray, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, int size)
    {
        createCompoundArray(objectPath, type, size,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize)
    {
        createCompoundArray(objectPath, type, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert type != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                            new long[]
                                { size }, new long[]
                                { blockSize }, type.getObjectByteifyer().getRecordSize(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert type != null;
        assert size >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                                new long[]
                                    { 0 }, new long[]
                                    { size }, type.getObjectByteifyer().getRecordSize(), registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                                new long[]
                                    { size }, null, type.getObjectByteifyer().getRecordSize(),
                                registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data)
    {
        writeCompoundMDArray(objectPath, type, data,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures features)
    {
        writeCompoundMDArray(objectPath, type, data, features, null);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures features,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, type.getStorageTypeId(), MDArray
                                    .toLong(data.dimensions()), type.getObjectByteifyer()
                                    .getRecordSize(), features, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockNumber)
    {
        writeCompoundMDArrayBlock(objectPath, type, data, blockNumber, null);
    }

    public <T> void writeCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockNumber,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert blockNumber != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        final long[] dataSetDimensions = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
            dataSetDimensions[i] = offset[i] + dimensions[i];
        }
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset)
    {
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long[] dimensions = data.longDimensions();
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
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset)
    {
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data, blockDimensions, offset,
                memoryOffset, null);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset, final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long[] memoryDimensions = data.longDimensions();
                    final long[] longBlockDimensions = MDArray.toLong(blockDimensions);
                    final long[] dataSetDimensions = new long[blockDimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + blockDimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, longBlockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(memoryDimensions, registry);
                    baseWriter.h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset),
                            longBlockDimensions);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions)
    {
        createCompoundMDArray(objectPath, type, dimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions)
    {
        createCompoundMDArray(objectPath, type, dimensions, blockDimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert type != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                            dimensions, MDArray.toLong(blockDimensions), type.getObjectByteifyer()
                                    .getRecordSize(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final int[] dimensions, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert type != null;
        assert dimensions != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    if (features.requiresChunking())
                    {
                        final long[] nullDimensions = new long[dimensions.length];
                        baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                                nullDimensions, MDArray.toLong(dimensions), type
                                        .getObjectByteifyer().getRecordSize(), registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, type.getStorageTypeId(), features,
                                MDArray.toLong(dimensions), null, type.getObjectByteifyer()
                                        .getRecordSize(), registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

}
