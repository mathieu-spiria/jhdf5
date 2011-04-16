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

import java.lang.reflect.Field;

import ch.systemsx.cisd.hdf5.HDF5ValueObjectByteifyer.FileInfoProvider;

/**
 * A {@link HDF5CompoundByteifyerFactory.IHDF5CompoundMemberBytifyerFactory} for
 * <code>HDF5EnumerationValueArray</code>.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundMemberByteifyerEnumArrayFactory implements IHDF5CompoundMemberBytifyerFactory
{

    public boolean canHandle(Class<?> clazz)
    {
        return (clazz == HDF5EnumerationValueArray.class);
    }

    public HDF5MemberByteifyer createBytifyer(final AccessType accessType, final Field fieldOrNull,
            final HDF5CompoundMemberMapping member, Class<?> memberClazz, final int index,
            final int offset, final FileInfoProvider fileInfoProvider)
    {
        final String memberName = member.getMemberName();
        final HDF5EnumerationType enumType = member.tryGetEnumerationType();
        if (enumType == null)
        {
            throw new NullPointerException("Enumeration type not set for member byteifyer.");
        }
        final int memberTypeLength = member.getMemberTypeLength();
        final int storageTypeId = member.getStorageDataTypeId();
        final int memberStorageTypeId =
                (storageTypeId < 0) ? fileInfoProvider.getArrayTypeId(enumType.getStorageTypeId(),
                        memberTypeLength) : storageTypeId;
        switch (accessType)
        {
            case FIELD:
                return createByteifyerForField(fieldOrNull, memberName, offset, enumType,
                        memberTypeLength, memberStorageTypeId, member.tryGetTypeVariant());
            case MAP:
                return createByteifyerForMap(memberName, offset, enumType, memberTypeLength,
                        memberStorageTypeId, member.tryGetTypeVariant());
            case LIST:
                return createByteifyerForList(memberName, index, offset, enumType,
                        memberTypeLength, memberStorageTypeId, member.tryGetTypeVariant());
            case ARRAY:
                return createByteifyerForArray(memberName, index, offset, enumType,
                        memberTypeLength, memberStorageTypeId, member.tryGetTypeVariant());
            default:
                throw new Error("Unknown access type");
        }
    }

    private HDF5MemberByteifyer createByteifyerForField(final Field field, final String memberName,
            final int offset, final HDF5EnumerationType enumType, final int memberTypeLength,
            final int memberStorageTypeId, final HDF5DataTypeVariant typeVariant)
    {
        ReflectionUtils.ensureAccessible(field);
        return new HDF5MemberByteifyer(field, memberName, enumType.getStorageForm()
                .getStorageSize() * memberTypeLength, offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return memberStorageTypeId;
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return -1;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnumArray(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final HDF5EnumerationValueArray enumValueArray =
                            HDF5EnumerationValueArray.fromStorageForm(enumType, byteArr,
                                    arrayOffset + offset, memberTypeLength);
                    field.set(obj, enumValueArray);
                }

                private HDF5EnumerationValueArray getEnumArray(Object obj)
                        throws IllegalAccessException, IllegalArgumentException
                {
                    assert obj != null;
                    return (HDF5EnumerationValueArray) field.get(obj);
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForMap(final String memberName, final int offset,
            final HDF5EnumerationType enumType, final int memberTypeLength,
            final int memberStorageTypeId, final HDF5DataTypeVariant typeVariant)
    {
        return new HDF5MemberByteifyer(null, memberName, enumType.getStorageForm().getStorageSize()
                * memberTypeLength, offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return memberStorageTypeId;
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return -1;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnumArray(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final HDF5EnumerationValueArray enumValueArray =
                            HDF5EnumerationValueArray.fromStorageForm(enumType, byteArr,
                                    arrayOffset + offset, memberTypeLength);
                    putMap(obj, memberName, enumValueArray);
                }

                private HDF5EnumerationValueArray getEnumArray(Object obj)
                        throws IllegalAccessException, IllegalArgumentException
                {
                    assert obj != null;
                    return (HDF5EnumerationValueArray) getMap(obj, memberName);
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForList(final String memberName, final int index,
            final int offset, final HDF5EnumerationType enumType, final int memberTypeLength,
            final int memberStorageTypeId, final HDF5DataTypeVariant typeVariant)
    {
        return new HDF5MemberByteifyer(null, memberName, enumType.getStorageForm().getStorageSize()
                * memberTypeLength, offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return memberStorageTypeId;
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return -1;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnumArray(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final HDF5EnumerationValueArray enumValueArray =
                            HDF5EnumerationValueArray.fromStorageForm(enumType, byteArr,
                                    arrayOffset + offset, memberTypeLength);
                    setList(obj, index, enumValueArray);
                }

                private HDF5EnumerationValueArray getEnumArray(Object obj)
                        throws IllegalAccessException, IllegalArgumentException
                {
                    assert obj != null;
                    return (HDF5EnumerationValueArray) getList(obj, index);
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForArray(final String memberName, final int index,
            final int offset, final HDF5EnumerationType enumType, final int memberTypeLength,
            final int memberStorageTypeId, final HDF5DataTypeVariant typeVariant)
    {
        return new HDF5MemberByteifyer(null, memberName, enumType.getStorageForm().getStorageSize()
                * memberTypeLength, offset, typeVariant)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return memberStorageTypeId;
                }

                @Override
                protected int getMemberNativeTypeId()
                {
                    return -1;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return getEnumArray(obj).toStorageForm();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final HDF5EnumerationValueArray enumValueArray =
                            HDF5EnumerationValueArray.fromStorageForm(enumType, byteArr,
                                    arrayOffset + offset, memberTypeLength);
                    setArray(obj, index, enumValueArray);
                }

                private HDF5EnumerationValueArray getEnumArray(Object obj)
                        throws IllegalAccessException, IllegalArgumentException
                {
                    assert obj != null;
                    return (HDF5EnumerationValueArray) getArray(obj, index);
                }
            };
    }

}
