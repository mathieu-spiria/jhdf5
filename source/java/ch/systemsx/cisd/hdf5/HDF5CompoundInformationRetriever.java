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
                            final CompoundTypeInformation compoundInformation =
                                    getCompoundTypeInformation(compoundDataTypeId, dataTypePath,
                                            registry);
                            return compoundInformation.members;
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException
    {
        return getCompoundDataSetInformation(dataSetPath, false);
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

    private CompoundTypeInformation getFullCompoundDataSetInformation(final String dataSetPath,
            final ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int dataSetId = baseReader.h5.openDataSet(baseReader.fileId, dataSetPath, registry);
        final int compoundDataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
        if (baseReader.h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
        {
            throw new HDF5JavaException("Data set '" + dataSetPath + "' is not of compound type.");
        }
        final String dataTypePathOrNull = baseReader.tryGetDataTypePath(compoundDataTypeId);
        final CompoundTypeInformation compoundInformation =
                getCompoundTypeInformation(compoundDataTypeId, dataTypePathOrNull, registry);
        return compoundInformation;
    }

    private CompoundTypeInformation getFullCompoundDataTypeInformation(final String dataTypePath,
            final ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int compoundDataTypeId =
                baseReader.h5.openDataType(baseReader.fileId, dataTypePath, registry);
        if (baseReader.h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
        {
            throw new HDF5JavaException("Data type '" + dataTypePath + "' is not a compound type.");
        }
        final CompoundTypeInformation compoundInformation =
                getCompoundTypeInformation(compoundDataTypeId, dataTypePath, registry);
        return compoundInformation;
    }

    private static final class CompoundTypeInformation
    {
        final String name;

        final int compoundDataTypeId;

        final HDF5CompoundMemberInformation[] members;

        final int[] dataTypeIds;

        CompoundTypeInformation(String name, int compoundDataTypeId, int length)
        {
            this.name = name;
            this.compoundDataTypeId = compoundDataTypeId;
            this.members = new HDF5CompoundMemberInformation[length];
            this.dataTypeIds = new int[length];
        }
    }

    CompoundTypeInformation getCompoundTypeInformation(final int compoundDataTypeId,
            final String dataTypePathOrNull, final ICleanUpRegistry registry)
    {
        final String typeName =
                HDF5Utils.getDataTypeNameFromPath(dataTypePathOrNull, HDF5DataClass.COMPOUND);
        final String[] memberNames =
                baseReader.h5.getNamesForEnumOrCompoundMembers(compoundDataTypeId);
        final CompoundTypeInformation compoundInfo =
                new CompoundTypeInformation(typeName, compoundDataTypeId, memberNames.length);
        int offset = 0;
        final HDF5DataTypeVariant[] memberTypeVariantsOrNull =
                baseReader.tryGetTypeVariantForCompoundMembers(dataTypePathOrNull, registry);
        if (memberTypeVariantsOrNull != null
                && memberTypeVariantsOrNull.length != memberNames.length)
        {
            throw new HDF5JavaException(
                    "Invalid member data type variant information on committed data type '"
                            + dataTypePathOrNull + "'.");
        }
        for (int i = 0; i < memberNames.length; ++i)
        {
            final int dataTypeId =
                    baseReader.h5.getDataTypeForIndex(compoundDataTypeId, i, registry);
            compoundInfo.dataTypeIds[i] = dataTypeId;
            final HDF5DataTypeInformation dataTypeInformation =
                    baseReader.getDataTypeInformation(dataTypeId, registry);
            if (memberTypeVariantsOrNull != null && memberTypeVariantsOrNull[i].isTypeVariant())
            {
                dataTypeInformation.setTypeVariant(memberTypeVariantsOrNull[i]);
            }
            final HDF5EnumerationType enumTypeOrNull;
            if (dataTypeInformation.getDataClass() == HDF5DataClass.ENUM)
            {
                if (dataTypeInformation.isArrayType())
                {
                    final int baseDataSetType = baseReader.h5.getBaseDataType(dataTypeId, registry);
                    enumTypeOrNull =
                            baseReader.getEnumTypeForStorageDataType(null, baseDataSetType, false,
                                    registry);
                } else
                {
                    enumTypeOrNull =
                            baseReader.getEnumTypeForStorageDataType(null, dataTypeId, false,
                                    registry);
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
        return getCompoundType(name, -1, pojoClass, objectArrayifyer);
    }

    private <T> HDF5CompoundType<T> getCompoundType(final String name, int committedDataTypeId,
            final Class<T> compoundType, final HDF5ValueObjectByteifyer<T> objectArrayifyer)
    {
        final int storageDataTypeId =
                (committedDataTypeId < 0) ? baseReader
                        .createStorageCompoundDataType(objectArrayifyer) : committedDataTypeId;
        final int nativeDataTypeId = baseReader.createNativeCompoundDataType(objectArrayifyer);
        return new HDF5CompoundType<T>(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                name, compoundType, objectArrayifyer);
    }

    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> pojoClass,
            final HDF5CompoundMemberMapping... members)
    {
        return getCompoundType(null, pojoClass, members);
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name,
            final Class<T> pojoClass)
    {
        return getCompoundType(name, pojoClass, HDF5CompoundMemberMapping.inferMapping(pojoClass));
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final Class<T> pojoClass)
    {
        return getInferredCompoundType(null, pojoClass);
    }

    @SuppressWarnings("unchecked")
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final T pojo)
    {
        if (Map.class.isInstance(pojo))
        {
            final String compoundTypeName =
                    (name == null) ? HDF5CompoundMemberMapping.constructCompoundTypeName(
                            ((Map) pojo).keySet(), true) : name;
            return (HDF5CompoundType<T>) getCompoundType(compoundTypeName, Map.class,
                    HDF5CompoundMemberMapping.inferMapping((Map) pojo));
        } else
        {
            final Class<T> pojoClass = (Class<T>) pojo.getClass();
            return getCompoundType(
                    name,
                    pojoClass,
                    HDF5CompoundMemberMapping.inferMapping(pojoClass,
                            HDF5CompoundMemberMapping.inferEnumerationTypeMap(pojo)));
        }
    }

    public <T> HDF5CompoundType<T> getInferredCompoundType(final T pojo)
    {
        return getInferredCompoundType(null, pojo);
    }

    public HDF5CompoundType<List<?>> getInferredCompoundType(List<String> memberNames, List<?> data)
    {
        return getInferredCompoundType(null, memberNames, data);
    }

    @SuppressWarnings("unchecked")
    public HDF5CompoundType<List<?>> getInferredCompoundType(String name, List<String> memberNames,
            List<?> data)
    {
        final String compoundTypeName =
                (name == null) ? HDF5CompoundMemberMapping.constructCompoundTypeName(memberNames,
                        false) : name;
        final HDF5CompoundType<?> type =
                getCompoundType(compoundTypeName, List.class,
                        HDF5CompoundMemberMapping.inferMapping(memberNames, data));
        return (HDF5CompoundType<List<?>>) type;
    }

    public HDF5CompoundType<Object[]> getInferredCompoundType(String[] memberNames, Object[] data)
    {
        return getInferredCompoundType(null, memberNames, data);
    }

    public HDF5CompoundType<Object[]> getInferredCompoundType(String name, String[] memberNames,
            Object[] data)
    {
        final String compoundTypeName =
                (name == null) ? HDF5CompoundMemberMapping.constructCompoundTypeName(
                        Arrays.asList(memberNames), false) : name;
        return getCompoundType(compoundTypeName, Object[].class,
                HDF5CompoundMemberMapping.inferMapping(memberNames, data));
    }

    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath, Class<T> pojoClass)
    {
        final CompoundTypeInformation cpdTypeInfo =
                getFullCompoundDataSetInformation(objectPath, baseReader.fileRegistry);
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(cpdTypeInfo.name, cpdTypeInfo.compoundDataTypeId, pojoClass,
                        createByteifyers(pojoClass, cpdTypeInfo));
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
        final CompoundTypeInformation cpdTypeInfo =
                getFullCompoundDataTypeInformation(dataTypePath, baseReader.fileRegistry);
        final HDF5CompoundType<T> typeForClass =
                getCompoundType(dataTypeName, cpdTypeInfo.compoundDataTypeId, pojoClass,
                        createByteifyers(pojoClass, cpdTypeInfo));
        return typeForClass;
    }

    private <T> HDF5ValueObjectByteifyer<T> createByteifyers(final Class<T> compoundClazz,
            final CompoundTypeInformation compoundMembers)
    {
        return baseReader.createCompoundByteifyers(compoundClazz,
                inferMemberMapping(compoundClazz, compoundMembers));
    }

    private HDF5CompoundMemberMapping[] inferMemberMapping(final Class<?> compoundClazz,
            final CompoundTypeInformation compoundTypeInfo)
    {
        final List<HDF5CompoundMemberMapping> mapping =
                new ArrayList<HDF5CompoundMemberMapping>(compoundTypeInfo.members.length);
        final Map<String, Field> fields = ReflectionUtils.getFieldMap(compoundClazz);
        for (int i = 0; i < compoundTypeInfo.members.length; ++i)
        {
            final HDF5CompoundMemberInformation compoundMember = compoundTypeInfo.members[i];
            final int compoundMemberTypeId = compoundTypeInfo.dataTypeIds[i];
            final Field fieldOrNull = fields.get(compoundMember.getName());
            final String memberName = compoundMember.getName();
            final String fieldName = (fieldOrNull != null) ? fieldOrNull.getName() : memberName;
            final HDF5DataTypeInformation typeInfo = compoundMember.getType();
            final int[] dimensions = typeInfo.getDimensions();
            if (typeInfo.getDataClass() == HDF5DataClass.ENUM)
            {
                if (dimensions.length == 0 || (dimensions.length == 1 && dimensions[0] == 1))
                {
                    if (fieldOrNull != null
                            && (fieldOrNull.getType() != HDF5EnumerationValue.class))
                    {
                        throw new HDF5JavaException(
                                "Field of enum type does not correspond to enumeration value");

                    }
                    mapping.add(HDF5CompoundMemberMapping
                            .mapping(memberName)
                            .fieldName(fieldName)
                            .enumType(
                                    new HDF5EnumerationType(baseReader.fileId,
                                            compoundMemberTypeId, baseReader.h5
                                                    .getNativeDataTypeCheckForBitField(
                                                            compoundMemberTypeId,
                                                            baseReader.fileRegistry), baseReader
                                                    .getEnumDataTypeName(compoundMember.getType()
                                                            .tryGetName(), compoundMemberTypeId),
                                            compoundMember.tryGetEnumValues()))
                            .typeVariant(typeInfo.tryGetTypeVariant()));
                } else if (dimensions.length == 1)
                {
                    if (fieldOrNull != null
                            && (fieldOrNull.getType() != HDF5EnumerationValueArray.class))
                    {
                        throw new HDF5JavaException(
                                "Field of enum type does not correspond to enumeration array value");

                    }
                    mapping.add(HDF5CompoundMemberMapping.mappingWithStorageTypeId(
                            fieldName,
                            memberName,
                            new HDF5EnumerationType(baseReader.fileId, -1, baseReader.h5
                                    .getNativeDataTypeCheckForBitField(compoundMemberTypeId,
                                            baseReader.fileRegistry), baseReader
                                    .getEnumDataTypeName(compoundMember.getType().tryGetName(),
                                            compoundMemberTypeId), compoundMember
                                    .tryGetEnumValues()), dimensions, compoundMemberTypeId,
                            typeInfo.tryGetTypeVariant()));
                }
            } else if (typeInfo.getDataClass() == HDF5DataClass.STRING)
            {
                if (fieldOrNull != null && (fieldOrNull.getType() != String.class)
                        && (fieldOrNull.getType() != char[].class))
                {
                    throw new HDF5JavaException(
                            "Field of string type does not correspond to string or char[] value");
                }
                mapping.add(HDF5CompoundMemberMapping.mappingArrayWithStorageId(fieldName,
                        memberName, String.class, new int[]
                            { typeInfo.getElementSize() }, compoundMemberTypeId,
                        typeInfo.tryGetTypeVariant()));

            } else
            {
                final Class<?> memberClazz;
                if (fieldOrNull != null)
                {
                    memberClazz = fieldOrNull.getType();
                } else
                {
                    memberClazz = typeInfo.tryGetJavaType();
                }
                mapping.add(HDF5CompoundMemberMapping.mappingArrayWithStorageId(fieldName,
                        memberName, memberClazz, dimensions, compoundMemberTypeId,
                        typeInfo.tryGetTypeVariant()));
            }
        }
        return mapping.toArray(new HDF5CompoundMemberMapping[mapping.size()]);
    }

}
