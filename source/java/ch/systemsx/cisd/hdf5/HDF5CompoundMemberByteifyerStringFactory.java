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

import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.getArray;
import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.getList;
import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.getMap;
import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.putMap;
import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.setArray;
import static ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.setList;

import java.lang.reflect.Field;

import ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.AccessType;
import ch.systemsx.cisd.hdf5.HDF5CompoundByteifyerFactory.IHDF5CompoundMemberBytifyerFactory;
import ch.systemsx.cisd.hdf5.HDF5ValueObjectByteifyer.FileInfoProvider;

/**
 * A {@link HDF5CompoundByteifyerFactory.IHDF5CompoundMemberBytifyerFactory} for <code>String</code>
 * .
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundMemberByteifyerStringFactory implements IHDF5CompoundMemberBytifyerFactory
{

    @Override
    public boolean canHandle(Class<?> clazz, HDF5CompoundMemberInformation memberInfoOrNull)
    {
        if (memberInfoOrNull != null)
        {
            return ((clazz == String.class) || (clazz == char[].class))
                    && memberInfoOrNull.getType().getDataClass() == HDF5DataClass.STRING;
        } else
        {
            return (clazz == String.class) || (clazz == char[].class);
        }
    }

    @Override
    public Class<?> tryGetOverrideJavaType(HDF5DataClass dataClass, int rank, int elementSize,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        return null;
    }

    @Override
    public HDF5MemberByteifyer createBytifyer(AccessType accessType, Field fieldOrNull,
            HDF5CompoundMemberMapping member,
            HDF5CompoundMemberInformation compoundMemberInfoOrNull,
            HDF5EnumerationType enumTypeOrNull, Class<?> memberClazz, int index, int offset,
            FileInfoProvider fileInfoProvider)
    {
        final String memberName = member.getMemberName();
        // May be -1 if not known
        final int memberTypeId = member.getStorageDataTypeId();
        final int maxLengthChars = member.getMemberTypeLength();
        final CharacterEncoding encoding = fileInfoProvider.getCharacterEncoding(memberTypeId); 
        final int maxLengthBytes =
                (compoundMemberInfoOrNull != null) ? compoundMemberInfoOrNull.getType().getSize()
                        : encoding.getMaxBytesPerChar()
                                * ((memberTypeId < 0) ? maxLengthChars + 1 : maxLengthChars);
        final int stringDataTypeId =
                (memberTypeId < 0) ? fileInfoProvider.getStringDataTypeId(maxLengthBytes)
                        : memberTypeId;
        final boolean isCharArray = (memberClazz == char[].class);
        switch (accessType)
        {
            case FIELD:
                return createByteifyerForField(fieldOrNull, memberName, offset, stringDataTypeId,
                        maxLengthChars, maxLengthBytes, encoding, isCharArray);
            case MAP:
                return createByteifyerForMap(memberName, offset, stringDataTypeId, maxLengthChars,
                        maxLengthBytes, encoding, isCharArray);
            case LIST:
                return createByteifyerForList(memberName, index, offset, stringDataTypeId,
                        maxLengthChars, maxLengthBytes, encoding, isCharArray);
            case ARRAY:
                return createByteifyerForArray(memberName, index, offset, stringDataTypeId,
                        maxLengthChars, maxLengthBytes, encoding, isCharArray);
            default:
                throw new Error("Unknown access type");
        }
    }

    private HDF5MemberByteifyer createByteifyerForField(final Field field, final String memberName,
            final int offset, final int stringDataTypeId, final int maxLength,
            final int maxLengthInBytes, final CharacterEncoding encoding, final boolean isCharArray)
    {
        ReflectionUtils.ensureAccessible(field);
        if (isCharArray)
        {
            return new HDF5MemberByteifyer(field, memberName, maxLength, maxLengthInBytes, offset,
                    encoding)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return stringDataTypeId;
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
                        final String s = new String((char[]) field.get(obj));
                        return StringUtils.toBytes0Term(s, getSize(), encoding);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final int totalOffset = arrayOffset + offset;
                        final int maxIdx = totalOffset + size;
                        field.set(obj,
                                StringUtils.fromBytes0Term(byteArr, totalOffset, maxIdx, encoding)
                                        .toCharArray());
                    }
                };
        } else
        {
            return new HDF5MemberByteifyer(field, memberName, maxLength, maxLengthInBytes, offset,
                    encoding)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return stringDataTypeId;
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
                        Object o = field.get(obj);
                        if (o == null)
                        {
                            throw new NullPointerException("Field '" + field.getName()
                                    + "' is null");

                        }
                        final String s = o.toString();
                        return StringUtils.toBytes0Term(s, getSize(), encoding);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final int totalOffset = arrayOffset + offset;
                        final int maxIdx = totalOffset + size;
                        field.set(obj,
                                StringUtils.fromBytes0Term(byteArr, totalOffset, maxIdx, encoding));
                    }
                };
        }
    }

    private HDF5MemberByteifyer createByteifyerForMap(final String memberName, final int offset,
            final int stringDataTypeId, final int maxLength, final int maxLengthInBytes,
            final CharacterEncoding encoding, final boolean isCharArray)
    {
        return new HDF5MemberByteifyer(null, memberName, maxLength, maxLengthInBytes, offset,
                encoding)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    final Object o = getMap(obj, memberName);
                    final String s;
                    if (o.getClass() == char[].class)
                    {
                        s = new String((char[]) o);
                    } else
                    {
                        s = o.toString();
                    }
                    return StringUtils.toBytes0Term(s, getSize(), encoding);
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offset;
                    final int maxIdx = totalOffset + size;
                    final String s =
                            StringUtils.fromBytes0Term(byteArr, totalOffset, maxIdx, encoding);
                    if (isCharArray)
                    {
                        putMap(obj, memberName, s.toCharArray());
                    } else
                    {
                        putMap(obj, memberName, s);
                    }
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForList(final String memberName, final int index,
            final int offset, final int stringDataTypeId, final int maxLength,
            final int maxLengthInBytes, final CharacterEncoding encoding, final boolean isCharArray)
    {
        return new HDF5MemberByteifyer(null, memberName, maxLength, maxLengthInBytes, offset,
                encoding)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    final Object o = getList(obj, index);
                    final String s;
                    if (o.getClass() == char[].class)
                    {
                        s = new String((char[]) o);
                    } else
                    {
                        s = o.toString();
                    }
                    return StringUtils.toBytes0Term(s, getSize(), encoding);
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offset;
                    final int maxIdx = totalOffset + size;
                    final String s =
                            StringUtils.fromBytes0Term(byteArr, totalOffset, maxIdx, encoding);
                    if (isCharArray)
                    {
                        setList(obj, index, s.toCharArray());
                    } else
                    {
                        setList(obj, index, s);
                    }
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForArray(final String memberName, final int index,
            final int offset, final int stringDataTypeId, final int maxLength,
            final int maxLengthInBytes, final CharacterEncoding encoding, final boolean isCharArray)
    {
        return new HDF5MemberByteifyer(null, memberName, maxLength, maxLengthInBytes, offset,
                encoding)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    final Object o = getArray(obj, index);
                    final String s;
                    if (o.getClass() == char[].class)
                    {
                        s = new String((char[]) o);
                    } else
                    {
                        s = o.toString();
                    }
                    return StringUtils.toBytes0Term(s, getSize(), encoding);
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offset;
                    final int maxIdx = totalOffset + size;
                    final String s =
                            StringUtils.fromBytes0Term(byteArr, totalOffset, maxIdx, encoding);
                    if (isCharArray)
                    {
                        setArray(obj, index, s.toCharArray());
                    } else
                    {
                        setArray(obj, index, s);
                    }
                }
            };
    }

}
