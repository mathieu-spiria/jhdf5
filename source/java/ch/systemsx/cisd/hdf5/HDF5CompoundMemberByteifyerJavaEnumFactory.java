/*
 * Copyright 2011 ETH Zuerich, CISD
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

import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.*;
import static ch.systemsx.cisd.hdf5.HDF5CompoundMappingHints.getEnumReturnType;

import java.lang.reflect.Field;

import ch.systemsx.cisd.hdf5.HDF5ValueObjectByteifyer.FileInfoProvider;

/**
 * A {@link HDF5CompoundByteifyerFactory.IHDF5CompoundMemberBytifyerFactory} for
 * <code>HDF5EnumerationValue</code>.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundMemberByteifyerJavaEnumFactory implements IHDF5CompoundMemberBytifyerFactory
{

    public boolean canHandle(Class<?> clazz)
    {
        return clazz.isEnum();
    }

    public Class<?> tryGetOverrideJavaType(HDF5DataClass dataClass, int rank, int elementSize,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        return null;
    }

    public HDF5MemberByteifyer createBytifyer(final AccessType accessType, final Field fieldOrNull,
            final HDF5CompoundMemberMapping member, Class<?> memberClazz, final int index,
            final int offset, final FileInfoProvider fileInfoProvider)
    {
        final String memberName = member.getMemberName();
        final HDF5EnumerationType enumType = member.tryGetEnumerationType();
        if (enumType == null)
        {
            throw new NullPointerException(
                    "Enumeration type not set for member byteifyer of field '" + memberName + "'.");
        }
        switch (accessType)
        {
            case FIELD:
                return createByteifyerForField(fieldOrNull, memberName, offset, enumType,
                        member.tryGetTypeVariant());
            case MAP:
                return createByteifyerForMap(memberName, offset, enumType,
                        member.tryGetTypeVariant(), getEnumReturnType(member));
            case LIST:
                return createByteifyerForList(memberName, index, offset, enumType,
                        member.tryGetTypeVariant(), getEnumReturnType(member));
            case ARRAY:
                return createByteifyerForArray(memberName, index, offset, enumType,
                        member.tryGetTypeVariant(), getEnumReturnType(member));
            default:
                throw new Error("Unknown access type");
        }
    }

    private HDF5MemberByteifyer createByteifyerForField(final Field field, final String memberName,
            final int offset, final HDF5EnumerationType enumType,
            final HDF5DataTypeVariant typeVariant)
    {
        ReflectionUtils.ensureAccessible(field);
        return new HDF5MemberByteifyer(field, memberName, enumType.getStorageForm()
                .getStorageSize(), offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return enumType.getStorageTypeId();
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return enumType.getNativeTypeId();
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnumOrdinal(obj);
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int ordinal =
                            enumType.getOrdinalFromStorageForm(byteArr, arrayOffset + offset);
                    field.set(obj, field.getType().getEnumConstants()[ordinal]);
                }

                private byte[] getEnumOrdinal(Object obj) throws IllegalAccessException,
                        IllegalArgumentException
                {
                    assert obj != null;
                    return new byte[]
                        { (byte) ((Enum<?>) field.get(obj)).ordinal() };
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForMap(final String memberName, final int offset,
            final HDF5EnumerationType enumType, final HDF5DataTypeVariant typeVariant,
            final HDF5CompoundMappingHints.EnumReturnType enumReturnType)
    {
        return new HDF5MemberByteifyer(null, memberName,
                enumType.getStorageForm().getStorageSize(), offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return enumType.getStorageTypeId();
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return enumType.getNativeTypeId();
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnum(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final Object enumValue =
                            getEnumValue(enumType, byteArr, arrayOffset + offset, enumReturnType);
                    putMap(obj, memberName, enumValue);
                }

                private HDF5EnumerationValue getEnum(Object obj) throws IllegalAccessException,
                        IllegalArgumentException
                {
                    assert obj != null;
                    final Object enumObj = getMap(obj, memberName);
                    if (enumObj instanceof HDF5EnumerationValue)
                    {
                        return (HDF5EnumerationValue) enumObj;
                    } else if (enumObj instanceof Number)
                    {
                        return new HDF5EnumerationValue(enumType, ((Number) enumObj).intValue());
                    } else
                    {
                        return new HDF5EnumerationValue(enumType, enumObj.toString());
                    }
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForList(final String memberName, final int index,
            final int offset, final HDF5EnumerationType enumType,
            final HDF5DataTypeVariant typeVariant,
            final HDF5CompoundMappingHints.EnumReturnType enumReturnType)
    {
        return new HDF5MemberByteifyer(null, memberName,
                enumType.getStorageForm().getStorageSize(), offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return enumType.getStorageTypeId();
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return enumType.getNativeTypeId();
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnum(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final Object enumValue =
                            getEnumValue(enumType, byteArr, arrayOffset + offset, enumReturnType);
                    setList(obj, index, enumValue);
                }

                private HDF5EnumerationValue getEnum(Object obj) throws IllegalAccessException,
                        IllegalArgumentException
                {
                    assert obj != null;
                    final Object enumObj = getList(obj, index);
                    if (enumObj instanceof HDF5EnumerationValue)
                    {
                        return (HDF5EnumerationValue) enumObj;
                    } else if (enumObj instanceof Number)
                    {
                        return new HDF5EnumerationValue(enumType, ((Number) enumObj).intValue());
                    } else
                    {
                        return new HDF5EnumerationValue(enumType, enumObj.toString());
                    }
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForArray(final String memberName, final int index,
            final int offset, final HDF5EnumerationType enumType,
            final HDF5DataTypeVariant typeVariant,
            final HDF5CompoundMappingHints.EnumReturnType enumReturnType)
    {
        return new HDF5MemberByteifyer(null, memberName,
                enumType.getStorageForm().getStorageSize(), offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return enumType.getStorageTypeId();
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return enumType.getNativeTypeId();
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnum(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final Object enumValue =
                            getEnumValue(enumType, byteArr, arrayOffset + offset, enumReturnType);
                    setArray(obj, index, enumValue);
                }

                private HDF5EnumerationValue getEnum(Object obj) throws IllegalAccessException,
                        IllegalArgumentException
                {
                    assert obj != null;
                    final Object enumObj = getArray(obj, index);
                    if (enumObj instanceof HDF5EnumerationValue)
                    {
                        return (HDF5EnumerationValue) enumObj;
                    } else if (enumObj instanceof Number)
                    {
                        return new HDF5EnumerationValue(enumType, ((Number) enumObj).intValue());
                    } else
                    {
                        return new HDF5EnumerationValue(enumType, enumObj.toString());
                    }
                }
            };
    }

    static Object getEnumValue(final HDF5EnumerationType enumType, byte[] byteArr, int arrayOffset,
            final HDF5CompoundMappingHints.EnumReturnType enumReturnType)
    {
        switch (enumReturnType)
        {
            case HDF5ENUMERATIONVALUE:
                return enumType.createFromStorageForm(byteArr, arrayOffset);
            case STRING:
                return enumType.createStringFromStorageForm(byteArr, arrayOffset);
            case ORDINAL:
                return enumType.getOrdinalFromStorageForm(byteArr, arrayOffset);
            default:
                throw new Error("Unknown EnumReturnType " + enumReturnType);
        }
    }

}
