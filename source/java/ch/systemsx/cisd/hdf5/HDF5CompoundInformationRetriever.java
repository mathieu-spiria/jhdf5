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
import java.util.List;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5CompoundInformationRetriever}.
 *
 * @author Bernd Rinn
 */
public class HDF5CompoundInformationRetriever implements IHDF5CompoundInformationRetriever
{

    protected final HDF5BaseReader baseReader;

    HDF5CompoundInformationRetriever(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
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

    protected static final class HDF5CompoundTypeInformation
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
            final HDF5EnumerationType enumTypeOrNull;
            if (dataTypeInformation.getDataClass() == HDF5DataClass.ENUM)
            {
                if (dataTypeInformation.isArrayType())
                {
                    final int baseDataSetType = baseReader.h5.getBaseDataType(dataTypeId, registry);
                    enumTypeOrNull =
                            baseReader.getEnumTypeForStorageDataType(baseDataSetType, registry);
                } else
                {
                    enumTypeOrNull = baseReader.getEnumTypeForStorageDataType(dataTypeId, registry);
                }
            } else
            {
                enumTypeOrNull = null;
            }
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

    public <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> pojoClass,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectArrayifyer =
                baseReader.createCompoundByteifyers(pojoClass, members);
        return getCompoundType(name, pojoClass, objectArrayifyer);
    }

    private <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> compoundType,
            final HDF5ValueObjectByteifyer<T> objectArrayifyer)
    {
        final int storageDataTypeId = baseReader.createStorageCompoundDataType(objectArrayifyer);
        final int nativeDataTypeId = baseReader.createNativeCompoundDataType(objectArrayifyer);
        return new HDF5CompoundType<T>(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                name, compoundType, objectArrayifyer);
    }

    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> pojoClass,
            final HDF5CompoundMemberMapping... members)
    {
        return getCompoundType(null, pojoClass, members);
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final Class<T> pojoClass)
    {
        return getCompoundType(name, pojoClass, HDF5CompoundMemberMapping
                .inferMapping(pojoClass));
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final Class<T> pojoClass)
    {
        return getInferredCompoundType(null, pojoClass);
    }

    @SuppressWarnings("unchecked")
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final T pojo)
    {
        final Class<T> pojoClass = (Class<T>) pojo.getClass();
        return getCompoundType(name, pojoClass, HDF5CompoundMemberMapping.inferMapping(pojoClass,
                HDF5CompoundMemberMapping.inferEnumerationTypeMap(pojo)));
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final T pojo)
    {
        return getInferredCompoundType(null, pojo);
    }

    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath,
            Class<T> pojoClass)
    {
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(null, pojoClass, createByteifyers(pojoClass,
                        getFullCompoundDataSetInformation(objectPath, baseReader.fileRegistry)));
        return typeForClass;
    }

    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> pojoClass)
    {
        return getNamedCompoundType(pojoClass.getSimpleName(), pojoClass);
    }

    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> pojoClass)
    {
        final String dataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName);
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(null, pojoClass, createByteifyers(pojoClass,
                        getFullCompoundDataTypeInformation(dataTypePath, baseReader.fileRegistry)));
        return typeForClass;
    }

    private <T> HDF5ValueObjectByteifyer<T> createByteifyers(final Class<T> compoundClazz,
            final HDF5CompoundTypeInformation compoundMembers)
    {
        return baseReader.createCompoundByteifyers(compoundClazz, inferMemberMapping(compoundClazz,
                compoundMembers));
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
                    if (fieldOrNull.getType() == HDF5EnumerationValue.class)
                    {
                        mapping.add(HDF5CompoundMemberMapping.mapping(fieldOrNull.getName(),
                                new HDF5EnumerationType(baseReader.fileId, compoundMemberTypeId,
                                        baseReader.h5.getNativeDataTypeCheckForBitField(
                                                compoundMemberTypeId, baseReader.fileRegistry),
                                        null, compoundMember.tryGetEnumValues())));
                    } else if (fieldOrNull.getType() == HDF5EnumerationValueArray.class)
                    {
                        mapping.add(HDF5CompoundMemberMapping.mapping(fieldOrNull.getName(),
                                new HDF5EnumerationType(baseReader.fileId, -1, baseReader.h5
                                        .getNativeDataTypeCheckForBitField(compoundMemberTypeId,
                                                baseReader.fileRegistry), null, compoundMember
                                        .tryGetEnumValues()), dimensions, compoundMemberTypeId));
                    } else
                    {
                        throw new HDF5JavaException(
                                "Field of enum type does not correspond to enumeration value");
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
                                .getName(), compoundMember.getName(), new int[]
                            { typeInfo.getElementSize() }, compoundMemberTypeId));
                    }
                } else
                {
                    mapping
                            .add(HDF5CompoundMemberMapping.mappingArrayWithStorageId(fieldOrNull
                                    .getName(), compoundMember.getName(), dimensions,
                                    compoundMemberTypeId));
                }
            }
        }
        return mapping.toArray(new HDF5CompoundMemberMapping[mapping.size()]);
    }

}
