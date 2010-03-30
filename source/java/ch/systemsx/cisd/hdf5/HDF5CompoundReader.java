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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_COMPOUND;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5CompoundReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundReader implements IHDF5CompoundReader
{

    private final HDF5BaseReader baseReader;

    HDF5CompoundReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    public <T> HDF5CompoundType<T> getCompoundTypeForDataSet(String objectPath,
            Class<T> compoundClass)
    {
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(null, compoundClass, createByteifyers(compoundClass,
                        getFullCompoundDataSetInformation(objectPath, baseReader.fileRegistry)));
        return typeForClass;
    }

    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> compoundClass)
    {
        return getNamedCompoundType(compoundClass.getSimpleName(), compoundClass);
    }

    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> compoundClass)
    {
        final String dataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName);
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(null, compoundClass, createByteifyers(compoundClass,
                        getFullCompoundDataTypeInformation(dataTypePath, baseReader.fileRegistry)));
        return typeForClass;
    }

    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass)
    {
        return getCompoundMemberInformation(compoundClass.getSimpleName());
    }

    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(final String dataTypeName)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> writeRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            final String dataTypePath =
                                    HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX,
                                            dataTypeName);
                            final int compoundDataTypeId =
                                    baseReader.h5.openDataType(baseReader.fileId, dataTypePath,
                                            registry);
                            final HDF5CompoundTypeInformation compoundInformation =
                                    getCompoundTypeInformation(compoundDataTypeId, registry);
                            Arrays.sort(compoundInformation.members);
                            return compoundInformation.members;
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException
    {
        return getCompoundDataSetInformation(dataSetPath, true);
    }

    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath,
            final boolean sortAlphabetically) throws HDF5JavaException
    {
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> infoRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            return getFullCompoundDataSetInformation(dataSetPath, registry).members;
                        }
                    };
        final HDF5CompoundMemberInformation[] compoundInformation =
                baseReader.runner.call(infoRunnable);
        if (sortAlphabetically)
        {
            Arrays.sort(compoundInformation);
        }
        return compoundInformation;
    }

    private HDF5CompoundTypeInformation getFullCompoundDataSetInformation(final String dataSetPath,
            final ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int dataSetId = baseReader.h5.openDataSet(baseReader.fileId, dataSetPath, registry);
        final int compoundDataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
        if (baseReader.h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
        {
            throw new HDF5JavaException("Data set '" + dataSetPath + "' is not of compound type.");
        }
        final HDF5CompoundTypeInformation compoundInformation =
                getCompoundTypeInformation(compoundDataTypeId, registry);
        return compoundInformation;
    }

    private HDF5CompoundTypeInformation getFullCompoundDataTypeInformation(
            final String dataTypePath, final ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int compoundDataTypeId =
                baseReader.h5.openDataType(baseReader.fileId, dataTypePath, registry);
        if (baseReader.h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
        {
            throw new HDF5JavaException("Data set '" + dataTypePath + "' is not of compound type.");
        }
        final HDF5CompoundTypeInformation compoundInformation =
                getCompoundTypeInformation(compoundDataTypeId, registry);
        return compoundInformation;
    }

    private static final class HDF5CompoundTypeInformation
    {
        final HDF5CompoundMemberInformation[] members;

        final int[] dataTypeIds;

        HDF5CompoundTypeInformation(int length)
        {
            members = new HDF5CompoundMemberInformation[length];
            dataTypeIds = new int[length];
        }
    }

    private HDF5CompoundTypeInformation getCompoundTypeInformation(final int compoundDataTypeId,
            final ICleanUpRegistry registry)
    {
        final String[] memberNames =
                baseReader.h5.getNamesForEnumOrCompoundMembers(compoundDataTypeId);
        final HDF5CompoundTypeInformation compoundInfo =
                new HDF5CompoundTypeInformation(memberNames.length);
        int offset = 0;
        for (int i = 0; i < memberNames.length; ++i)
        {
            final int dataTypeId =
                    baseReader.h5.getDataTypeForIndex(compoundDataTypeId, i, registry);
            compoundInfo.dataTypeIds[i] = dataTypeId;
            final HDF5DataTypeInformation dataTypeInformation =
                    baseReader.getDataTypeInformation(dataTypeId);
            final HDF5EnumerationType enumTypeOrNull =
                    (dataTypeInformation.getDataClass() == HDF5DataClass.ENUM) ? baseReader
                            .getEnumTypeForStorageDataType(dataTypeId, registry) : null;
            if (enumTypeOrNull != null)
            {
                compoundInfo.members[i] =
                        new HDF5CompoundMemberInformation(memberNames[i], dataTypeInformation,
                                offset, enumTypeOrNull.getValueArray());
            } else
            {
                compoundInfo.members[i] =
                        new HDF5CompoundMemberInformation(memberNames[i], dataTypeInformation,
                                offset);
            }
            offset += compoundInfo.members[i].getType().getSize();
        }
        return compoundInfo;
    }

    public <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectArrayifyer =
                baseReader.createCompoundByteifyers(compoundType, members);
        return getCompoundType(name, compoundType, objectArrayifyer);
    }

    private <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> compoundType,
            final HDF5ValueObjectByteifyer<T> objectArrayifyer)
    {
        final int storageDataTypeId = baseReader.createStorageCompoundDataType(objectArrayifyer);
        final int nativeDataTypeId = baseReader.createNativeCompoundDataType(objectArrayifyer);
        return new HDF5CompoundType<T>(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                name, compoundType, objectArrayifyer);
    }

    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        return getCompoundType(null, compoundType, members);
    }

    public <T> T readCompound(String objectPath, Class<T> compoundClass) throws HDF5JavaException
    {
        final HDF5CompoundType<T> typeForClass =
                getCompoundTypeForDataSet(objectPath, compoundClass);
        return readCompound(objectPath, typeForClass, null);
    }

    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return readCompound(objectPath, type, null);
    }

    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompound(objectPath, -1, -1, type, inspectorOrNull);
    }

    public <T> T[] readCompoundArray(String objectPath, Class<T> compoundClass)
            throws HDF5JavaException
    {
        final HDF5CompoundType<T> typeForClass =
                getCompoundTypeForDataSet(objectPath, compoundClass);
        return readCompoundArray(objectPath, typeForClass, null);
    }

    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return readCompoundArray(objectPath, type, null);
    }

    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, -1, -1, type, inspectorOrNull);
    }

    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber) throws HDF5JavaException
    {
        return readCompoundArrayBlock(objectPath, type, blockSize, blockNumber, null);
    }

    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, blockSize * blockNumber, type,
                inspectorOrNull);
    }

    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset)
            throws HDF5JavaException
    {
        return readCompoundArrayBlockWithOffset(objectPath, type, blockSize, offset, null);
    }

    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, offset, type, inspectorOrNull);
    }

    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return getCompoundArrayNaturalBlocks(objectPath, type, null);
    }

    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5DataSetInformation info = baseReader.getDataSetInformation(objectPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == HDF5StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<T[]>>()
            {
                public Iterator<HDF5DataBlock<T[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<T[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<T[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final T[] block =
                                        readCompoundArrayBlockWithOffset(objectPath, type,
                                                blockSize, offset, inspectorOrNull);
                                return new HDF5DataBlock<T[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    private <T> T primReadCompound(final String objectPath, final int blockSize, final long offset,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        final ICallableWithCleanUp<T> writeRunnable = new ICallableWithCleanUp<T>()
            {
                public T call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArr);
                    }
                    return type.getObjectByteifyer().arrayifyScalar(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private <T> T[] primReadCompoundArray(final String objectPath, final int blockSize,
            final long offset, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        final ICallableWithCleanUp<T[]> writeRunnable = new ICallableWithCleanUp<T[]>()
            {
                public T[] call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArr);
                    }
                    return type.getObjectByteifyer().arrayify(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private void checkCompoundType(final int dataTypeId, final String path,
            final ICleanUpRegistry registry)
    {
        final boolean isCompound = (baseReader.h5.getClassType(dataTypeId) == H5T_COMPOUND);
        if (isCompound == false)
        {
            throw new HDF5JavaException(path + " needs to be a Compound.");
        }
    }

    private HDF5CompoundMemberMapping[] inferMemberMapping(final Class<?> compoundClazz,
            final HDF5CompoundTypeInformation compoundTypeInfo)
    {
        final List<HDF5CompoundMemberMapping> mapping =
                new ArrayList<HDF5CompoundMemberMapping>(compoundTypeInfo.members.length);
        final Map<String, Field> fields = ReflectionUtils.getFieldMap(compoundClazz);
        for (int i = 0; i < compoundTypeInfo.members.length; ++i)
        {
            final HDF5CompoundMemberInformation compoundMember = compoundTypeInfo.members[i];
            final int compoundMemberTypeId = compoundTypeInfo.dataTypeIds[i];
            final Field fieldOrNull = fields.get(compoundMember.getName());
            if (fieldOrNull != null)
            {
                final HDF5DataTypeInformation typeInfo = compoundMember.getType();
                final int[] dimensions = typeInfo.getDimensions();
                if (typeInfo.getDataClass() == HDF5DataClass.ENUM)
                {
                    if (fieldOrNull.getType() != HDF5EnumerationValue.class)
                    {
                        throw new HDF5JavaException(
                                "Field of enum type does not correspond to enumeration value");
                    } else
                    {
                        mapping.add(HDF5CompoundMemberMapping.mapping(fieldOrNull.getName(),
                                new HDF5EnumerationType(baseReader.fileId, compoundMemberTypeId,
                                        baseReader.h5.getNativeDataTypeCheckForBitField(
                                                compoundMemberTypeId, baseReader.fileRegistry),
                                        null, compoundMember.tryGetEnumValues())));
                    }
                } else if (typeInfo.getDataClass() == HDF5DataClass.STRING)
                {
                    if (fieldOrNull.getType() != String.class
                            && fieldOrNull.getType() != char[].class)
                    {
                        throw new HDF5JavaException(
                                "Field of string type does not correspond to string or char[] value");
                    } else
                    {
                        mapping.add(HDF5CompoundMemberMapping.mappingArrayWithStorageId(fieldOrNull
                                .getName(), new int[]
                            { typeInfo.getElementSize() }, compoundMemberTypeId));
                    }
                } else
                {
                    mapping.add(HDF5CompoundMemberMapping.mappingArrayWithStorageId(fieldOrNull
                            .getName(), dimensions, compoundMemberTypeId));
                }
            }
        }
        return mapping.toArray(new HDF5CompoundMemberMapping[mapping.size()]);
    }

    private <T> HDF5ValueObjectByteifyer<T> createByteifyers(final Class<T> compoundClazz,
            final HDF5CompoundTypeInformation compoundMembers)
    {
        return baseReader.createCompoundByteifyers(compoundClazz, inferMemberMapping(compoundClazz,
                compoundMembers));
    }

    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return readCompoundMDArray(objectPath, type, null);
    }

    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, null, null, inspectorOrNull);
    }

    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber)
            throws HDF5JavaException
    {
        return readCompoundMDArrayBlock(objectPath, type, blockDimensions, blockNumber, null);
    }

    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockDimensions[i] * blockNumber[i];
        }
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset,
                inspectorOrNull);
    }

    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset)
            throws HDF5JavaException
    {
        return readCompoundMDArrayBlockWithOffset(objectPath, type, blockDimensions, offset, null);
    }

    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset,
                inspectorOrNull);
    }

    private <T> MDArray<T> primReadCompoundArrayRankN(final String objectPath,
            final HDF5CompoundType<T> type, final int[] dimensionsOrNull,
            final long[] offsetOrNull, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        final ICallableWithCleanUp<MDArray<T>> writeRunnable =
                new ICallableWithCleanUp<MDArray<T>>()
                    {
                        public MDArray<T> call(final ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int storageDataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            checkCompoundType(storageDataTypeId, objectPath, registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offsetOrNull,
                                            dimensionsOrNull, registry);
                            final int nativeDataTypeId = type.getNativeTypeId();
                            final byte[] byteArr =
                                    new byte[spaceParams.blockSize
                                            * type.getObjectByteifyer().getRecordSize()];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                            if (inspectorOrNull != null)
                            {
                                inspectorOrNull.inspect(byteArr);
                            }
                            return new MDArray<T>(type.getObjectByteifyer().arrayify(
                                    storageDataTypeId, byteArr, type.getCompoundType()),
                                    spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

}
