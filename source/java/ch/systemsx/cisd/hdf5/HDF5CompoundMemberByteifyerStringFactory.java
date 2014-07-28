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
import ch.systemsx.cisd.hdf5.hdf5lib.HDFNativeData;

/**
 * A {@link HDF5CompoundByteifyerFactory.IHDF5CompoundMemberBytifyerFactory} for <code>String</code>
 * .
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundMemberByteifyerStringFactory implements IHDF5CompoundMemberBytifyerFactory
{

    private static abstract class HDF5StringMemberByteifyer extends HDF5MemberByteifyer
    {

        HDF5StringMemberByteifyer(Field fieldOrNull, String memberName, int size, int offset,
                int memOffset, CharacterEncoding encoding, int maxCharacters,
                boolean isVariableLengthType)
        {
            super(fieldOrNull, memberName, size, offset, memOffset, encoding, maxCharacters,
                    isVariableLengthType);
        }

        /**
         * For strings, this is the <i>minimal</i> element size 1 for fixed strings or the size of a
         * pointer for variable-length strings.
         */
        @Override
        int getElementSize()
        {
            return isVariableLengthType() ? HDFNativeData.getMachineWordSize() : 1;
        }

        @Override
        public boolean mayBeCut()
        {
            return true;
        }

        @Override
        protected int getMemberNativeTypeId()
        {
            return -1;
        }
    }

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
            int memOffset, FileInfoProvider fileInfoProvider)
    {
        final String memberName = member.getMemberName();
        final int maxCharacters = member.getMemberTypeLength();
        final boolean isVariableLengthType =
                member.isVariableLength()
                        || (maxCharacters == 0 && member.tryGetHints() != null && member
                                .tryGetHints().isUseVariableLengthStrings());
        // May be -1 if not known
        final int memberTypeId =
                isVariableLengthType ? fileInfoProvider.getVariableLengthStringDataTypeId()
                        : member.getStorageDataTypeId();
        final CharacterEncoding encoding = fileInfoProvider.getCharacterEncoding(memberTypeId);
        final int size =
                (compoundMemberInfoOrNull != null) ? compoundMemberInfoOrNull.getType().getSize()
                        : encoding.getMaxBytesPerChar() * maxCharacters;
        final int stringDataTypeId =
                (memberTypeId < 0) ? fileInfoProvider.getStringDataTypeId(size) : memberTypeId;
        final boolean isCharArray = (memberClazz == char[].class);
        switch (accessType)
        {
            case FIELD:
                return createByteifyerForField(fieldOrNull, memberName, offset, memOffset,
                        stringDataTypeId, maxCharacters, size, encoding, isCharArray,
                        isVariableLengthType);
            case MAP:
                return createByteifyerForMap(memberName, offset, memOffset, stringDataTypeId,
                        maxCharacters, size, encoding, isCharArray, isVariableLengthType);
            case LIST:
                return createByteifyerForList(memberName, index, offset, memOffset,
                        stringDataTypeId, maxCharacters, size, encoding, isCharArray,
                        isVariableLengthType);
            case ARRAY:
                return createByteifyerForArray(memberName, index, offset, memOffset,
                        stringDataTypeId, maxCharacters, size, encoding, isCharArray,
                        isVariableLengthType);
            default:
                throw new Error("Unknown access type");
        }
    }

    private HDF5MemberByteifyer createByteifyerForField(final Field field, final String memberName,
            final int offset, int memOffset, final int stringDataTypeId, final int maxCharacters,
            final int size, final CharacterEncoding encoding, final boolean isCharArray,
            final boolean isVariableLengthType)
    {
        ReflectionUtils.ensureAccessible(field);
        return new HDF5StringMemberByteifyer(field, memberName, size, offset, memOffset, encoding,
                maxCharacters, isVariableLengthType)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    Object o = field.get(obj);
                    if (o == null)
                    {
                        throw new NullPointerException("Field '" + field.getName() + "' is null");

                    }
                    final String s = isCharArray ? new String((char[]) o) : o.toString();
                    if (isVariableLengthType)
                    {
                        final byte[] result = new byte[HDFNativeData.getMachineWordSize()];
                        HDFNativeData.compoundCpyVLStr(s, result, 0);
                        return result;
                    } else
                    {
                        return StringUtils.toBytes0Term(s, getMaxCharacters(), encoding);
                    }
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offsetInMemory;
                    final int maxIdx = totalOffset + maxCharacters;
                    final String s =
                            isVariableLengthType ? HDFNativeData.createVLStrFromCompound(byteArr,
                                    totalOffset) : StringUtils.fromBytes0Term(byteArr, totalOffset,
                                    maxIdx, encoding);
                    field.set(obj, isCharArray ? s.toCharArray() : s);
                }
            };
    }

    private HDF5MemberByteifyer createByteifyerForMap(final String memberName, final int offset,
            int memOffset, final int stringDataTypeId, final int maxCharacters, final int size,
            final CharacterEncoding encoding, final boolean isCharArray,
            final boolean isVariableLengthType)
    {
        return new HDF5StringMemberByteifyer(null, memberName, size, offset, memOffset, encoding,
                maxCharacters, isVariableLengthType)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    if (isVariableLengthType)
                    {
                        final byte[] result = new byte[HDFNativeData.getMachineWordSize()];
                        HDFNativeData.compoundCpyVLStr(s, result, 0);
                        return result;
                    } else
                    {
                        return StringUtils.toBytes0Term(s, getMaxCharacters(), encoding);
                    }
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offsetInMemory;
                    final int maxIdx = totalOffset + maxCharacters;
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
            final int offset, int memOffset, final int stringDataTypeId, final int maxCharacters,
            final int size, final CharacterEncoding encoding, final boolean isCharArray,
            final boolean isVariableLengthType)
    {
        return new HDF5StringMemberByteifyer(null, memberName, size, offset, memOffset, encoding,
                maxCharacters, isVariableLengthType)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    if (isVariableLengthType)
                    {
                        final byte[] result = new byte[HDFNativeData.getMachineWordSize()];
                        HDFNativeData.compoundCpyVLStr(s, result, 0);
                        return result;
                    } else
                    {
                        return StringUtils.toBytes0Term(s, getMaxCharacters(), encoding);
                    }
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offsetInMemory;
                    final int maxIdx = totalOffset + maxCharacters;
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
            final int offset, int memOffset, final int stringDataTypeId, final int maxCharacters,
            final int size, final CharacterEncoding encoding, final boolean isCharArray,
            final boolean isVariableLengthType)
    {
        return new HDF5StringMemberByteifyer(null, memberName, size, offset, memOffset, encoding,
                maxCharacters, isVariableLengthType)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return stringDataTypeId;
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
                    if (isVariableLengthType)
                    {
                        final byte[] result = new byte[HDFNativeData.getMachineWordSize()];
                        HDFNativeData.compoundCpyVLStr(s, result, 0);
                        return result;
                    } else
                    {
                        return StringUtils.toBytes0Term(s, getMaxCharacters(), encoding);
                    }
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offsetInMemory;
                    final int maxIdx = totalOffset + maxCharacters;
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
