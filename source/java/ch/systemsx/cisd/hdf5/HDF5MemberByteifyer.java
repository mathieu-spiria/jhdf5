/*
 * Copyright 2008 ETH Zuerich, CISD
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

import static ncsa.hdf.hdf5lib.H5.H5Tinsert;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_B64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I16LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I8LE;
import static ch.systemsx.cisd.base.convert.NativeData.DOUBLE_SIZE;
import static ch.systemsx.cisd.base.convert.NativeData.FLOAT_SIZE;
import static ch.systemsx.cisd.base.convert.NativeData.INT_SIZE;
import static ch.systemsx.cisd.base.convert.NativeData.LONG_SIZE;
import static ch.systemsx.cisd.base.convert.NativeData.SHORT_SIZE;

import java.lang.reflect.Field;
import java.util.BitSet;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.hdf5.HDF5ValueObjectByteifyer.FileInfoProvider;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A class that byteifies member fields of objects.
 * 
 * @author Bernd Rinn
 */
abstract class HDF5MemberByteifyer
{
    private final Field field;

    private final String memberName;

    protected final int size;

    protected final int offset;

    static HDF5MemberByteifyer createBooleanMemberByteifyer(final Field field,
            final String memberName, final int booleanDataTypeId, final int offset)
    {
        setAccessible(field);
        return new HDF5MemberByteifyer(field, memberName, 1, offset)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return booleanDataTypeId;
                }

                @Override
                public byte[] byteify(int compoundDataTypeId, Object obj)
                        throws IllegalAccessException
                {
                    return HDFNativeData.byteToByte((byte) (field.getBoolean(obj) ? 1 : 0));
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    field.setBoolean(obj, byteArr[arrayOffset + offset] == 0 ? false : true);
                }
            };
    }

    static HDF5MemberByteifyer createEnumMemberByteifyer(final Field field,
            final String memberName, final HDF5EnumerationType enumType, final int offset)
    {
        setAccessible(field);
        return new HDF5MemberByteifyer(field, memberName, enumType.getStorageForm()
                .getStorageSize(), offset)
            {
                @Override
                protected int getMemberStorageTypeId()
                {
                    return enumType.getStorageTypeId();
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
                    final HDF5EnumerationValue enumValue =
                            enumType.createFromStorageForm(byteArr, arrayOffset + offset);
                    field.set(obj, enumValue);
                }

                private HDF5EnumerationValue getEnum(Object obj) throws IllegalAccessException,
                        IllegalArgumentException
                {
                    assert obj != null;
                    return (HDF5EnumerationValue) field.get(obj);
                }
            };
    }

    static HDF5MemberByteifyer createStringMemberByteifyer(final Field field,
            final String memberName, final int offset, final int stringDataTypeId,
            final int maxLength)
    {
        setAccessible(field);
        return new HDF5MemberByteifyer(field, memberName, maxLength + 1, offset)
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
                    String s = field.get(obj).toString();
                    if (s.length() >= getSize())
                    {
                        s = s.substring(0, getSize() - 1);
                    }
                    return (s + '\0').getBytes();
                }

                @Override
                public void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
                        int arrayOffset) throws IllegalAccessException
                {
                    final int totalOffset = arrayOffset + offset;
                    final int maxIdx = totalOffset + size;
                    int termIdx;
                    for (termIdx = totalOffset; termIdx < maxIdx && byteArr[termIdx] != 0; ++termIdx)
                    {
                    }
                    field.set(obj, new String(byteArr, totalOffset, termIdx - totalOffset));
                }
            };
    }

    static HDF5MemberByteifyer createArrayMemberByteifyer(final Field field,
            final String memberName, final int offset, final FileInfoProvider fileInfoProvider,
            final int len)
    {
        setAccessible(field);
        final Class<?> memberClazz = field.getType();
        if (memberClazz == byte[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_STD_I8LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return (byte[]) field.get(obj);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final byte[] array = new byte[len];
                        System.arraycopy(byteArr, arrayOffset + offset, array, 0, array.length);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == short[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len * SHORT_SIZE, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_STD_I16LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final short[] array = (short[]) field.get(obj);
                        return HDFNativeData.shortToByte(array);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final short[] array =
                                HDFNativeData.byteToShort(byteArr, arrayOffset + offset, len);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == int[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len * INT_SIZE, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_STD_I32LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final int[] array = (int[]) field.get(obj);
                        return HDFNativeData.intToByte(array);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final int[] array =
                                HDFNativeData.byteToInt(byteArr, arrayOffset + offset, len);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == long[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len * LONG_SIZE, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_STD_I64LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final long[] array = (long[]) field.get(obj);
                        return HDFNativeData.longToByte(array);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final long[] array =
                                HDFNativeData.byteToLong(byteArr, arrayOffset + offset, len);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == float[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len * FLOAT_SIZE, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_IEEE_F32LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final float[] array = (float[]) field.get(obj);
                        return HDFNativeData.floatToByte(array);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final float[] array =
                                HDFNativeData.byteToFloat(byteArr, arrayOffset + offset, len);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == double[].class)
        {
            return new HDF5MemberByteifyer(field, memberName, len * LONG_SIZE, offset)
                {
                    final int memberTypeId = fileInfoProvider.getArrayTypeId(H5T_IEEE_F64LE, len);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final double[] array = (double[]) field.get(obj);
                        return HDFNativeData.doubleToByte(array);
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final double[] array =
                                HDFNativeData.byteToDouble(byteArr, arrayOffset + offset, len);
                        field.set(obj, array);
                    }
                };
        } else if (memberClazz == BitSet.class)
        {
            final int lenInLongs = len / 64 + (len % 64 != 0 ? 1 : 0);

            if (lenInLongs <= 0)
            {
                throw new IllegalArgumentException(
                        "Length of a bit field must be a positive number.");
            }
            return new HDF5MemberByteifyer(field, memberName, lenInLongs * LONG_SIZE, offset)
                {
                    final int memberTypeId =
                            fileInfoProvider.getArrayTypeId(H5T_STD_B64LE, lenInLongs);

                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return memberTypeId;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        final BitSet bs = (BitSet) field.get(obj);
                        return HDFNativeData.longToByte(BitSetConversionUtils.toStorageForm(bs));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        final BitSet bs =
                                BitSetConversionUtils.fromStorageForm(HDFNativeData.byteToLong(
                                        byteArr, arrayOffset + offset, lenInLongs));
                        field.set(obj, bs);
                    }
                };
        } else
        {
            throw new IllegalArgumentException("The field '" + field.getName() + "' is of type '"
                    + memberClazz.getCanonicalName()
                    + "' which cannot be handled by an HDFMemberByteifyer.");
        }
    }

    static HDF5MemberByteifyer createMemberByteifyer(final Field field, final String memberName,
            int offset)
    {
        setAccessible(field);
        final Class<?> memberClazz = field.getType();
        if (memberClazz == byte.class)
        {
            return new HDF5MemberByteifyer(field, memberName, 1, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_STD_I8LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.byteToByte(field.getByte(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setByte(obj, byteArr[arrayOffset + offset]);
                    }
                };
        } else if (memberClazz == short.class)
        {
            return new HDF5MemberByteifyer(field, memberName, SHORT_SIZE, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_STD_I16LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.shortToByte(field.getShort(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setShort(obj, HDFNativeData
                                .byteToShort(byteArr, arrayOffset + offset));
                    }
                };
        } else if (memberClazz == int.class)
        {
            return new HDF5MemberByteifyer(field, memberName, INT_SIZE, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_STD_I32LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.intToByte(field.getInt(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setInt(obj, HDFNativeData.byteToInt(byteArr, arrayOffset + offset));
                    }
                };
        } else if (memberClazz == long.class)
        {
            return new HDF5MemberByteifyer(field, memberName, LONG_SIZE, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_STD_I64LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.longToByte(field.getLong(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setLong(obj, HDFNativeData.byteToLong(byteArr, arrayOffset + offset));
                    }
                };
        } else if (memberClazz == float.class)
        {
            return new HDF5MemberByteifyer(field, memberName, FLOAT_SIZE, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_IEEE_F32LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.floatToByte(field.getFloat(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setFloat(obj, HDFNativeData
                                .byteToFloat(byteArr, arrayOffset + offset));
                    }
                };
        } else if (memberClazz == double.class)
        {
            return new HDF5MemberByteifyer(field, memberName, DOUBLE_SIZE, offset)
                {
                    @Override
                    protected int getMemberStorageTypeId()
                    {
                        return H5T_IEEE_F64LE;
                    }

                    @Override
                    public byte[] byteify(int compoundDataTypeId, Object obj)
                            throws IllegalAccessException
                    {
                        return HDFNativeData.doubleToByte(field.getDouble(obj));
                    }

                    @Override
                    public void setFromByteArray(int compoundDataTypeId, Object obj,
                            byte[] byteArr, int arrayOffset) throws IllegalAccessException
                    {
                        field.setDouble(obj, HDFNativeData.byteToDouble(byteArr, arrayOffset
                                + offset));
                    }
                };
        } else
        {
            throw new IllegalArgumentException("The field '" + field.getName() + "' is of type '"
                    + memberClazz.getCanonicalName()
                    + "' which cannot be handled by an HDFMemberByteifyer.");
        }
    }

    private static void setAccessible(Field field)
    {
        if (field.isAccessible() == false)
        {
            field.setAccessible(true);
        }
    }

    private HDF5MemberByteifyer(Field field, String memberName, int size, int offset)
    {
        this.field = field;
        this.memberName = memberName;
        this.size = size;
        this.offset = offset;
    }

    public abstract byte[] byteify(int compoundDataTypeId, Object obj)
            throws IllegalAccessException;

    public abstract void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
            int arrayOffset) throws IllegalAccessException;

    protected abstract int getMemberStorageTypeId();

    public final void insertType(int dataTypeId)
    {
        H5Tinsert(dataTypeId, memberName, offset, getMemberStorageTypeId());
    }

    public final void insertNativeType(int dataTypeId, HDF5 h5, ICleanUpRegistry registry)
    {
        H5Tinsert(dataTypeId, memberName, offset, h5.getNativeDataTypeCheckForBitField(
                getMemberStorageTypeId(), registry));
    }

    public final int getSize()
    {
        return size;
    }

    public final int getOffset()
    {
        return offset;
    }

    public final int getTotalSize()
    {
        return offset + size;
    }

    public final String describe()
    {
        return "field '" + field.getName() + "' of class '"
                + field.getDeclaringClass().getCanonicalName() + "'";
    }
}
