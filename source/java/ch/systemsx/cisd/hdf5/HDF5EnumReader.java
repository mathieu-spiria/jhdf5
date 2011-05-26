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

import static ch.systemsx.cisd.hdf5.HDF5Utils.ENUM_PREFIX;
import static ch.systemsx.cisd.hdf5.HDF5Utils.createDataTypePath;
import static ch.systemsx.cisd.hdf5.HDF5Utils.getOneDimensionalArraySize;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ENUM;

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.HDF5EnumerationType.StorageFormEnum;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5EnumReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5EnumReader implements IHDF5EnumReader
{
    private static final int MIN_ENUM_SIZE_FOR_UPFRONT_LOADING = 10;

    protected final HDF5BaseReader baseReader;

    HDF5EnumReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    // /////////////////////
    // Types
    // /////////////////////

    public HDF5EnumerationType getEnumType(final String name)
    {
        baseReader.checkOpen();
        final String dataTypePath = createDataTypePath(ENUM_PREFIX, name);
        final int storageDataTypeId = baseReader.getDataTypeId(dataTypePath);
        return baseReader.getEnumTypeForStorageDataType(name, storageDataTypeId,
                baseReader.fileRegistry);
    }

    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return getEnumType(name, values, true);
    }

    public HDF5EnumerationType getEnumType(final String name, final String[] values,
            final boolean check) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5EnumerationType dataType = getEnumType(name);
        if (check)
        {
            baseReader.checkEnumValues(dataType.getStorageTypeId(), values, name);
        }
        return dataType;
    }

    public HDF5EnumerationType getEnumTypeForObject(final String dataSetPath)
    {
        return getDataSetEnumType(dataSetPath);
    }
    
    public HDF5EnumerationType getDataSetEnumType(final String dataSetPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationType> readEnumTypeCallable =
                new ICallableWithCleanUp<HDF5EnumerationType>()
                    {
                        public HDF5EnumerationType call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, dataSetPath,
                                            registry);
                            return getEnumTypeForDataSetId(dataSetId, dataSetPath,
                                    baseReader.isScaledEnum(dataSetId, registry), registry);
                        }
                    };
        return baseReader.runner.call(readEnumTypeCallable);
    }

    private HDF5EnumerationType getEnumTypeForDataSetId(final int objectId,
            final String objectName, final boolean scaledEnum, final ICleanUpRegistry registry)
    {
        if (scaledEnum)
        {
            final String enumTypeName =
                    baseReader.getStringAttribute(objectId, objectName,
                            HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE, registry);
            return getEnumType(enumTypeName);
        } else
        {
            final int storageDataTypeId =
                    baseReader.h5.getDataTypeForDataSet(objectId, baseReader.fileRegistry);
            return baseReader.getEnumTypeForStorageDataType(null, storageDataTypeId,
                    baseReader.fileRegistry);
        }
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public String getEnumAttributeAsString(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForAttribute(attributeId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);

                    final int enumDataTypeId =
                            baseReader.getEnumDataTypeId(storageDataTypeId, registry);
                    final int size = baseReader.h5.getDataTypeSize(enumDataTypeId);
                    final byte[] data =
                            baseReader.h5.readAttributeAsByteArray(attributeId, nativeDataTypeId,
                                    size);
                    final String value =
                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(enumDataTypeId,
                                    HDF5EnumerationType.fromStorageForm(data, 0, size));
                    if (value == null)
                    {
                        throw new HDF5JavaException("Attribute " + attributeName + " of object "
                                + objectPath + " needs to be an Enumeration.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    public HDF5EnumerationValue getEnumAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final int storageDataTypeId =
                                    baseReader.h5.getDataTypeForAttribute(attributeId,
                                            baseReader.fileRegistry);
                            final int enumTypeId =
                                    baseReader.getEnumDataTypeId(storageDataTypeId,
                                            baseReader.fileRegistry);
                            final HDF5EnumerationType enumType =
                                    baseReader.getEnumTypeForStorageDataType(null, enumTypeId,
                                            baseReader.fileRegistry);
                            final int nativeDataTypeId;
                            if (storageDataTypeId != enumTypeId) // Array data type
                            {
                                nativeDataTypeId =
                                        baseReader.h5
                                                .getNativeDataType(storageDataTypeId, registry);
                            } else
                            {
                                nativeDataTypeId = enumType.getNativeTypeId();
                            }
                            final int enumOrdinal =
                                    baseReader.getEnumOrdinal(attributeId, nativeDataTypeId,
                                            enumType);
                            return new HDF5EnumerationValue(enumType, enumOrdinal);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    public HDF5EnumerationValueArray getEnumArrayAttribute(final String objectPath,
            final String attributeName) throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValueArray> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValueArray>()
                    {
                        public HDF5EnumerationValueArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            return baseReader.getEnumValueArray(attributeId, registry);
                        }

                    };
        return baseReader.runner.call(readRunnable);
    }

    public String[] getEnumArrayAttributeAsString(final String objectPath,
            final String attributeName) throws HDF5JavaException
    {
        final HDF5EnumerationValueArray array = getEnumArrayAttribute(objectPath, attributeName);
        final String[] result = new String[array.getLength()];
        for (int i = 0; i < result.length; ++i)
        {
            result[i] = array.getValue(i);
        }
        return result;
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public String readEnumAsString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);
                    final int size = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    final byte[] data = new byte[size];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                    final String value =
                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(storageDataTypeId,
                                    HDF5EnumerationType.fromStorageForm(data));
                    if (value == null)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be an Enumeration.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    public HDF5EnumerationValue readEnum(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5EnumerationType enumType =
                                    getEnumTypeForDataSetId(dataSetId, objectPath, false, registry);
                            return readEnumValue(dataSetId, enumType);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    public HDF5EnumerationValue readEnum(final String objectPath, final HDF5EnumerationType enumType)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert enumType != null;

        baseReader.checkOpen();
        enumType.check(baseReader.fileId);
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            return readEnumValue(dataSetId, enumType);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    private HDF5EnumerationValue readEnumValue(final int dataSetId,
            final HDF5EnumerationType enumType)
    {
        final byte[] data = new byte[enumType.getStorageForm().getStorageSize()];
        baseReader.h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
        return new HDF5EnumerationValue(enumType, HDF5EnumerationType.fromStorageForm(data));
    }

    public HDF5EnumerationValueArray readEnumArray(final String objectPath,
            final HDF5EnumerationType enumTypeOrNull) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValueArray> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValueArray>()
                    {
                        public HDF5EnumerationValueArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                            final boolean scaledEnum = baseReader.isScaledEnum(dataSetId, registry);
                            final HDF5EnumerationType actualEnumType =
                                    (enumTypeOrNull == null) ? getEnumTypeForDataSetId(dataSetId,
                                            objectPath, scaledEnum, registry) : enumTypeOrNull;
                            final int arraySize = HDF5Utils.getOneDimensionalArraySize(dimensions);
                            final StorageFormEnum storageForm = actualEnumType.getStorageForm();
                            final byte[] data = new byte[arraySize * storageForm.getStorageSize()];
                            if (scaledEnum)
                            {
                                baseReader.h5.readDataSet(dataSetId, actualEnumType
                                        .getStorageForm().getIntNativeTypeId(), data);
                            } else
                            {
                                baseReader.h5.readDataSet(dataSetId,
                                        actualEnumType.getNativeTypeId(), data);
                            }
                            return new HDF5EnumerationValueArray(actualEnumType,
                                    HDF5EnumerationType.fromStorageForm(data, storageForm));
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    public HDF5EnumerationValueArray readEnumArray(final String objectPath)
            throws HDF5JavaException
    {
        return readEnumArray(objectPath, null);
    }

    public String[] readEnumArrayAsString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                    final int vectorLength = getOneDimensionalArraySize(dimensions);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);
                    final HDF5EnumerationType enumTypeOrNull =
                            tryGetEnumTypeForResolution(dataSetId, objectPath, nativeDataTypeId,
                                    vectorLength, registry);
                    final int size = baseReader.h5.getDataTypeSize(nativeDataTypeId);

                    final String[] values = new String[vectorLength];
                    final byte[] data = new byte[vectorLength * size];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                    if (enumTypeOrNull != null)
                    {
                        for (int i = 0; i < vectorLength; ++i)
                        {
                            values[i] =
                                    enumTypeOrNull.getValueArray()[HDF5EnumerationType
                                            .fromStorageForm(data, i, size)];
                        }
                    } else
                    {
                        for (int i = 0; i < vectorLength; ++i)
                        {
                            values[i] =
                                    baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                            storageDataTypeId,
                                            HDF5EnumerationType.fromStorageForm(data, i, size));
                        }
                    }
                    return values;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private HDF5EnumerationType tryGetEnumTypeForResolution(final int dataSetId,
            final String objectPath, final int nativeDataTypeId,
            final int numberOfEntriesToResolve, ICleanUpRegistry registry)
    {
        final boolean nativeEnum = (baseReader.h5.getClassType(nativeDataTypeId) == H5T_ENUM);
        final boolean scaledEnum =
                nativeEnum ? false : baseReader.isScaledEnum(dataSetId, registry);
        if (nativeEnum == false && scaledEnum == false)
        {
            throw new HDF5JavaException(objectPath + " is not an enum.");
        }
        if (scaledEnum || numberOfEntriesToResolve >= MIN_ENUM_SIZE_FOR_UPFRONT_LOADING)
        {
            return getEnumTypeForDataSetId(dataSetId, objectPath, scaledEnum, registry);
        }
        return null;
    }

    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(final String objectPath,
            final HDF5EnumerationType enumTypeOrNull, final int blockSize, final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValueArray> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValueArray>()
                    {
                        public HDF5EnumerationValueArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset, blockSize,
                                            registry);
                            final boolean scaledEnum = baseReader.isScaledEnum(dataSetId, registry);
                            final HDF5EnumerationType actualEnumType =
                                    (enumTypeOrNull == null) ? getEnumTypeForDataSetId(dataSetId,
                                            objectPath, scaledEnum, registry) : enumTypeOrNull;
                            final byte[] data =
                                    new byte[spaceParams.blockSize
                                            * actualEnumType.getStorageForm().getStorageSize()];
                            if (scaledEnum)
                            {
                                baseReader.h5.readDataSet(dataSetId, actualEnumType
                                        .getStorageForm().getIntNativeTypeId(),
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            } else
                            {
                                baseReader.h5.readDataSet(dataSetId,
                                        actualEnumType.getNativeTypeId(),
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            }
                            return new HDF5EnumerationValueArray(actualEnumType,
                                    HDF5EnumerationType.fromStorageForm(data,
                                            actualEnumType.getStorageForm()));
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset)
    {
        return readEnumArrayBlockWithOffset(objectPath, null, blockSize, offset);
    }

    public HDF5EnumerationValueArray readEnumArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber)
    {
        return readEnumArrayBlockWithOffset(objectPath, null, blockSize, blockNumber * blockSize);
    }

    public HDF5EnumerationValueArray readEnumArrayBlock(final String objectPath,
            final HDF5EnumerationType enumType, final int blockSize, final long blockNumber)
    {
        return readEnumArrayBlockWithOffset(objectPath, enumType, blockSize, blockNumber
                * blockSize);
    }

    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            final String objectPath, final HDF5EnumerationType enumTypeOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(objectPath));

        return new Iterable<HDF5DataBlock<HDF5EnumerationValueArray>>()
            {
                public Iterator<HDF5DataBlock<HDF5EnumerationValueArray>> iterator()
                {
                    return new Iterator<HDF5DataBlock<HDF5EnumerationValueArray>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            public HDF5DataBlock<HDF5EnumerationValueArray> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final HDF5EnumerationValueArray block =
                                        readEnumArrayBlockWithOffset(objectPath, enumTypeOrNull,
                                                index.getBlockSize(), offset);
                                return new HDF5DataBlock<HDF5EnumerationValueArray>(block,
                                        index.getAndIncIndex(), offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException
    {
        return getEnumArrayNaturalBlocks(objectPath, null);
    }

}
