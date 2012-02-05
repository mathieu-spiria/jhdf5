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

import static ch.systemsx.cisd.hdf5.hdf5lib.H5D.H5Dwrite;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5S_ALL;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation.DataTypeInfoOptions;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5CompoundWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundWriter extends HDF5CompoundReader implements IHDF5CompoundWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5CompoundWriter(HDF5BaseWriter baseWriter, IHDF5EnumWriter enumWriter)
    {
        super(baseWriter, enumWriter);
        this.baseWriter = baseWriter;
    }

    @Override
    public <T> HDF5CompoundType<T> getType(final String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members)
    {
        baseWriter.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectByteifyer =
                baseWriter.createCompoundByteifyers(pojoClass, members);
        final String dataTypeName = (name != null) ? name : deriveCompoundNameFromClass(pojoClass);
        final int storageDataTypeId =
                getOrCreateCompoundDataType(dataTypeName, pojoClass, objectByteifyer,
                        baseWriter.keepDataSetIfExists);
        final int nativeDataTypeId = baseWriter.createNativeCompoundDataType(objectByteifyer);
        return new HDF5CompoundType<T>(baseWriter.fileId, storageDataTypeId, nativeDataTypeId,
                dataTypeName, pojoClass, objectByteifyer,
                new HDF5CompoundType.IHDF5InternalCompoundMemberInformationRetriever()
                    {
                        public HDF5CompoundMemberInformation[] getCompoundMemberInformation(
                                final DataTypeInfoOptions dataTypeOptions)
                        {
                            return HDF5CompoundWriter.this.getCompoundMemberInformation(
                                    storageDataTypeId, name, dataTypeOptions);
                        }
                    });
    }

    private <T> String deriveCompoundNameFromClass(Class<T> pojoClass)
    {
        final CompoundType ct = pojoClass.getAnnotation(CompoundType.class);
        final String name = (ct != null) ? ct.name() : "";
        return name.length() == 0 ? pojoClass.getSimpleName() : name;
    }

    private <T> int getOrCreateCompoundDataType(final String dataTypeName,
            final Class<T> compoundClass, final HDF5ValueObjectByteifyer<T> objectByteifyer,
            boolean committedDataTypeHasPreference)
    {
        final String dataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName);
        final int committedStorageDataTypeId = baseWriter.getDataTypeId(dataTypePath);
        final boolean typeExists = (committedStorageDataTypeId >= 0);
        int storageDataTypeId = committedStorageDataTypeId;
        final boolean commitType;
        if ((typeExists == false) || (committedDataTypeHasPreference == false))
        {
            storageDataTypeId = baseWriter.createStorageCompoundDataType(objectByteifyer);
            final boolean typesAreEqual =
                    typeExists
                            && baseWriter.h5.dataTypesAreEqual(committedStorageDataTypeId,
                                    storageDataTypeId);
            commitType = (typeExists == false) || (typesAreEqual == false);
            if (typeExists && commitType)
            {
                final String replacementDataTypePath = baseWriter.moveLinkOutOfTheWay(dataTypePath);
                baseReader.renameNamedDataType(dataTypePath, replacementDataTypePath);
            }
            if (typesAreEqual)
            {
                storageDataTypeId = committedStorageDataTypeId;
            }
        } else
        {
            commitType = false;
        }
        if (commitType)
        {
            baseWriter.commitDataType(dataTypePath, storageDataTypeId);
            final HDF5EnumerationValueArray typeVariants =
                    tryCreateDataTypeVariantArray(objectByteifyer);
            if (typeVariants != null)
            {
                baseWriter.setEnumArrayAttribute(dataTypePath,
                        HDF5Utils.TYPE_VARIANT_MEMBERS_ATTRIBUTE, typeVariants);
            }
        }
        return storageDataTypeId;
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredType(String name, Class<T> pojoClass)
    {
        if (baseWriter.keepDataSetIfExists == false)
        {
            return super.getInferredType(name, pojoClass);
        } else
        {
            final String dataTypeName = (name != null) ? name : pojoClass.getSimpleName();
            final boolean typeExists =
                    baseReader.h5.exists(baseReader.fileId,
                            HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName));
            if (typeExists)
            {
                return getNamedType(dataTypeName, pojoClass);
            } else
            {
                return super.getInferredType(dataTypeName, pojoClass);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> HDF5CompoundType<T> getInferredType(final String name, final T pojo)
    {
        if (baseWriter.keepDataSetIfExists == false)
        {
            return super.getInferredType(name, pojo);
        } else
        {
            final String dataTypeName = (name != null) ? name : pojo.getClass().getSimpleName();
            final boolean typeExists =
                    baseReader.h5.exists(baseReader.fileId,
                            HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName));
            if (typeExists)
            {
                return (HDF5CompoundType<T>) getNamedType(dataTypeName, pojo.getClass());
            } else
            {
                return super.getInferredType(dataTypeName, pojo);
            }
        }
    }

    private <T> HDF5EnumerationValueArray tryCreateDataTypeVariantArray(
            final HDF5ValueObjectByteifyer<T> objectByteifyer)
    {
        final byte[] typeVariantOrdinals = new byte[objectByteifyer.getByteifyers().length];
        boolean hasTypeVariants = false;
        for (int i = 0; i < typeVariantOrdinals.length; ++i)
        {
            typeVariantOrdinals[i] =
                    (byte) objectByteifyer.getByteifyers()[i].getTypeVariant().ordinal();
            hasTypeVariants |= HDF5DataTypeVariant.isTypeVariant(typeVariantOrdinals[i]);
        }
        return hasTypeVariants ? new HDF5EnumerationValueArray(baseWriter.typeVariantDataType,
                typeVariantOrdinals) : null;
    }

    public <T> void setAttr(final String objectPath, final String attributeName,
            final HDF5CompoundType<T> type, final T data)
    {
        primSetCompoundAttribute(objectPath, attributeName, type, data, null);
    }

    public <T> void setAttr(final String objectPath, final String attributeName,
            final T data)
    {
        final HDF5CompoundType<T> inferredCompoundType = getInferredType(data);
        inferredCompoundType.checkMappingComplete();
        primSetCompoundAttribute(objectPath, attributeName, inferredCompoundType, data, null);
    }

    private <T> void primSetCompoundAttribute(final String objectPath, final String attributeName,
            final HDF5CompoundType<?> type, final T data, final IByteArrayInspector inspectorOrNull)
    {
        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        @SuppressWarnings("unchecked")
        final byte[] byteArray =
                ((HDF5CompoundType<T>) type).getObjectByteifyer().byteify(type.getStorageTypeId(),
                        data);
        if (inspectorOrNull != null)
        {
            inspectorOrNull.inspect(byteArray);
        }
        baseWriter.setAttribute(objectPath, attributeName, type.getStorageTypeId(),
                type.getNativeTypeId(), byteArray);
    }

    public <T> void write(final String objectPath, final HDF5CompoundType<T> type,
            final T data)
    {
        primWriteCompound(objectPath, type, data, null);
    }

    public <T> void write(final String objectPath, final HDF5CompoundType<T> type,
            final T data, final IByteArrayInspector inspectorOrNull)
    {
        primWriteCompound(objectPath, type, data, inspectorOrNull);
    }

    private <T> void primWriteCompound(final String objectPath, final HDF5CompoundType<?> type,
            final T data, final IByteArrayInspector inspectorOrNull)
    {
        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        @SuppressWarnings("unchecked")
        final byte[] byteArray =
                ((HDF5CompoundType<T>) type).getObjectByteifyer().byteify(type.getStorageTypeId(),
                        data);
        if (inspectorOrNull != null)
        {
            inspectorOrNull.inspect(byteArray);
        }
        baseWriter.writeScalar(objectPath, type.getStorageTypeId(), type.getNativeTypeId(),
                byteArray);
    }

    public <T> void write(String objectPath, T data)
    {
        final HDF5CompoundType<T> inferredCompoundType = getInferredType(data);
        inferredCompoundType.checkMappingComplete();
        primWriteCompound(objectPath, inferredCompoundType, data, null);
    }

    public <T> void writeArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data)
    {
        primWriteCompoundArray(objectPath, type, data,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION, null);
    }

    public <T> void writeArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures features)
    {
        primWriteCompoundArray(objectPath, type, data, features, null);
    }

    public <T> void writeArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures features,
            final IByteArrayInspector inspectorOrNull)
    {
        primWriteCompoundArray(objectPath, type, data, features, inspectorOrNull);
    }

    private <T> void primWriteCompoundArray(final String objectPath,
            final HDF5CompoundType<?> type, final T[] data,
            final HDF5GenericStorageFeatures features, final IByteArrayInspector inspectorOrNull)
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
                            baseWriter.getOrCreateDataSetId(objectPath, type.getStorageTypeId(),
                                    new long[]
                                        { data.length }, type.getObjectByteifyer().getRecordSize(),
                                    features, registry);
                    @SuppressWarnings("unchecked")
                    final byte[] byteArray =
                            ((HDF5CompoundType<T>) type).getObjectByteifyer().byteify(
                                    type.getStorageTypeId(), data);
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

    public <T> void writeArray(String objectPath, T[] data)
    {
        writeArray(objectPath, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeArray(String objectPath, T[] data,
            HDF5GenericStorageFeatures features)
    {
        assert data != null && data.length > 0;

        final HDF5CompoundType<T> inferredCompoundType = getInferredType(data[0]);
        inferredCompoundType.checkMappingComplete();
        primWriteCompoundArray(objectPath, inferredCompoundType, data, features, null);
    }

    public <T> void writeArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber)
    {
        writeArrayBlock(objectPath, type, data, blockNumber, null);
    }

    public <T> void writeArrayBlock(final String objectPath,
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

    public <T> void writeArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset)
    {
        writeArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeArrayBlockWithOffset(final String objectPath,
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

    public <T> void createArray(String objectPath, HDF5CompoundType<T> type, int size)
    {
        createArray(objectPath, type, size,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize)
    {
        createArray(objectPath, type, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createArray(final String objectPath, final HDF5CompoundType<T> type,
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

    public <T> void createArray(final String objectPath, final HDF5CompoundType<T> type,
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

    public <T> void writeMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data)
    {
        writeMDArray(objectPath, type, data,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures features)
    {
        writeMDArray(objectPath, type, data, features, null);
    }

    public <T> void writeMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures features,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        primWriteCompoundMDArray(objectPath, type, data, features, inspectorOrNull);
    }

    private <T> void primWriteCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data,
            final HDF5GenericStorageFeatures features, final IByteArrayInspector inspectorOrNull)
    {
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getOrCreateDataSetId(objectPath, type.getStorageTypeId(),
                                    MDArray.toLong(data.dimensions()), type.getObjectByteifyer()
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

    public <T> void writeMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockNumber)
    {
        writeMDArrayBlock(objectPath, type, data, blockNumber, null);
    }

    public <T> void writeMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockNumber,
            final IByteArrayInspector inspectorOrNull)
    {
        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        final long[] dataSetDimensions = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
            dataSetDimensions[i] = offset[i] + dimensions[i];
        }
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data.getAsFlatArray(), dimensions,
                offset, dataSetDimensions, inspectorOrNull);
    }

    public <T> void writeMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset)
    {
        writeMDArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset,
            final IByteArrayInspector inspectorOrNull)
    {
        final long[] dimensions = data.longDimensions();
        final long[] dataSetDimensions = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            dataSetDimensions[i] = offset[i] + dimensions[i];
        }
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data.getAsFlatArray(), dimensions,
                offset, dataSetDimensions, inspectorOrNull);
    }

    private <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long[] dimensions,
            final long[] offset, final long[] dataSetDimensions,
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
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
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

    public <T> void writeMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset)
    {
        writeMDArrayBlockWithOffset(objectPath, type, data, blockDimensions, offset,
                memoryOffset, null);
    }

    public <T> void writeMDArrayBlockWithOffset(final String objectPath,
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

    public <T> void createMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions)
    {
        createMDArray(objectPath, type, dimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions)
    {
        createMDArray(objectPath, type, dimensions, blockDimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createMDArray(final String objectPath, final HDF5CompoundType<T> type,
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

    public <T> void createMDArray(final String objectPath, final HDF5CompoundType<T> type,
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

    public <T> void writeMDArray(String objectPath, MDArray<T> data)
    {
        writeMDArray(objectPath, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeMDArray(String objectPath, MDArray<T> data,
            HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null && data.size() > 0;

        baseWriter.checkOpen();
        final HDF5CompoundType<T> inferredCompoundType =
                getInferredType(data.getAsFlatArray()[0]);
        inferredCompoundType.checkMappingComplete();
        primWriteCompoundMDArray(objectPath, inferredCompoundType, data, features, null);
    }

}
