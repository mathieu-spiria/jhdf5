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

import java.util.Iterator;

/**
 * A class the represents an array of HDF enumeration values.
 * 
 * @author Bernd Rinn
 */
public class HDF5EnumerationValueArray implements Iterable<String>
{

    private final HDF5EnumerationType type;

    private final int length;

    private byte[] bArrayOrNull;

    private short[] sArrayOrNull;

    private int[] iArrayOrNull;

    HDF5EnumerationValueArray(HDF5EnumerationType type, Object array)
            throws IllegalArgumentException
    {
        this.type = type;
        if (array instanceof byte[])
        {
            final byte[] bArray = (byte[]) array;
            this.length = bArray.length;
            setOrdinalArray(bArray);
        } else if (array instanceof short[])
        {
            final short[] sArray = (short[]) array;
            this.length = sArray.length;
            setArray(sArray);
        } else if (array instanceof int[])
        {
            final int[] iArray = (int[]) array;
            this.length = iArray.length;
            setArray(iArray);
        } else
        {
            throw new IllegalArgumentException("array is of illegal type "
                    + array.getClass().getCanonicalName());
        }
    }

    /**
     * Creates an enumeration value array.
     * 
     * @param type The enumeration type of this value.
     * @param ordinalArray The array of ordinal values in the <var>type</var>.
     * @throws IllegalArgumentException If any of the ordinals in the <var>ordinalArray</var> is
     *             outside of the range of allowed values of the <var>type</var>.
     */
    public HDF5EnumerationValueArray(HDF5EnumerationType type, byte[] ordinalArray)
            throws IllegalArgumentException
    {
        this.type = type;
        this.length = ordinalArray.length;
        setOrdinalArray(ordinalArray);
    }

    /**
     * Creates an enumeration value array.
     * 
     * @param type The enumeration type of this value.
     * @param ordinalArray The array of ordinal values in the <var>type</var>.
     * @throws IllegalArgumentException If any of the ordinals in the <var>ordinalArray</var> is
     *             outside of the range of allowed values of the <var>type</var>.
     */
    public HDF5EnumerationValueArray(HDF5EnumerationType type, short[] ordinalArray)
            throws IllegalArgumentException
    {
        this.type = type;
        this.length = ordinalArray.length;
        setArray(ordinalArray);
    }

    /**
     * Creates an enumeration value array.
     * 
     * @param type The enumeration type of this value.
     * @param ordinalArray The array of ordinal values in the <var>type</var>.
     * @throws IllegalArgumentException If any of the ordinals in the <var>ordinalArray</var> is
     *             outside of the range of allowed values of the <var>type</var>.
     */
    public HDF5EnumerationValueArray(HDF5EnumerationType type, int[] ordinalArray)
            throws IllegalArgumentException
    {
        this.type = type;
        this.length = ordinalArray.length;
        setArray(ordinalArray);
    }

    /**
     * Creates an enumeration value array.
     * 
     * @param type The enumeration type of this value.
     * @param valueArray The array of string values (each one needs to be one of the values of
     *            <var>type</var>).
     * @throws IllegalArgumentException If any of the values in the <var>valueArray</var> is not one
     *             of the values of <var>type</var>.
     */
    public HDF5EnumerationValueArray(HDF5EnumerationType type, String[] valueArray)
            throws IllegalArgumentException
    {
        this.type = type;
        this.length = valueArray.length;
        map(valueArray);
    }

    private void map(String[] array) throws IllegalArgumentException
    {
        if (type.getValueArray().length < Byte.MAX_VALUE)
        {
            bArrayOrNull = new byte[array.length];
            for (int i = 0; i < array.length; ++i)
            {
                final Integer indexOrNull = type.tryGetIndexForValue(array[i]);
                if (indexOrNull == null)
                {
                    throw new IllegalArgumentException("Value '" + array[i]
                            + "' is not allowed for type '" + type.getName() + "'.");
                }
                bArrayOrNull[i] = indexOrNull.byteValue();
            }
            sArrayOrNull = null;
            iArrayOrNull = null;
        } else if (type.getValueArray().length < Short.MAX_VALUE)
        {
            bArrayOrNull = null;
            sArrayOrNull = new short[array.length];
            for (int i = 0; i < array.length; ++i)
            {
                final Integer indexOrNull = type.tryGetIndexForValue(array[i]);
                if (indexOrNull == null)
                {
                    throw new IllegalArgumentException("Value '" + array[i]
                            + "' is not allowed for type '" + type.getName() + "'.");
                }
                sArrayOrNull[i] = indexOrNull.shortValue();
            }
            iArrayOrNull = null;
        } else
        {
            bArrayOrNull = null;
            sArrayOrNull = null;
            iArrayOrNull = new int[array.length];
            for (int i = 0; i < array.length; ++i)
            {
                final Integer indexOrNull = type.tryGetIndexForValue(array[i]);
                if (indexOrNull == null)
                {
                    throw new IllegalArgumentException("Value '" + array[i]
                            + "' is not allowed for type '" + type.getName() + "'.");
                }
                iArrayOrNull[i] = indexOrNull.intValue();
            }
        }
    }

    private void setOrdinalArray(byte[] array)
    {
        if (type.getValueArray().length < Byte.MAX_VALUE)
        {
            bArrayOrNull = array;
            checkOrdinalArray(bArrayOrNull);
            sArrayOrNull = null;
            iArrayOrNull = null;
        } else if (type.getValueArray().length < Short.MAX_VALUE)
        {
            bArrayOrNull = null;
            sArrayOrNull = toShortArray(array);
            checkOrdinalArray(sArrayOrNull);
            iArrayOrNull = null;
        } else
        {
            bArrayOrNull = null;
            sArrayOrNull = null;
            iArrayOrNull = toIntArray(array);
            checkOrdinalArray(iArrayOrNull);
        }
    }

    private void setArray(short[] array) throws IllegalArgumentException
    {
        if (type.getValueArray().length < Byte.MAX_VALUE)
        {
            bArrayOrNull = toByteArray(array);
            checkOrdinalArray(bArrayOrNull);
            sArrayOrNull = null;
            iArrayOrNull = null;
        } else if (type.getValueArray().length < Short.MAX_VALUE)
        {
            bArrayOrNull = null;
            sArrayOrNull = array;
            checkOrdinalArray(sArrayOrNull);
            iArrayOrNull = null;
        } else
        {
            bArrayOrNull = null;
            sArrayOrNull = null;
            iArrayOrNull = toIntArray(array);
            checkOrdinalArray(iArrayOrNull);
        }
    }

    private void setArray(int[] array) throws IllegalArgumentException
    {
        if (type.getValueArray().length < Byte.MAX_VALUE)
        {
            bArrayOrNull = toByteArray(array);
            checkOrdinalArray(bArrayOrNull);
            sArrayOrNull = null;
            iArrayOrNull = null;
        } else if (type.getValueArray().length < Short.MAX_VALUE)
        {
            bArrayOrNull = null;
            sArrayOrNull = toShortArray(array);
            checkOrdinalArray(sArrayOrNull);
            iArrayOrNull = null;
        } else
        {
            bArrayOrNull = null;
            sArrayOrNull = null;
            iArrayOrNull = array;
            checkOrdinalArray(iArrayOrNull);
        }
    }

    private byte[] toByteArray(short[] array) throws IllegalArgumentException
    {
        final byte[] bArray = new byte[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            bArray[i] = (byte) array[i];
            if (bArray[i] != array[i])
            {
                throw new IllegalArgumentException("Value " + array[i]
                        + " cannot be stored in byte array");
            }
        }
        return bArray;
    }

    private byte[] toByteArray(int[] array) throws IllegalArgumentException
    {
        final byte[] bArray = new byte[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            bArray[i] = (byte) array[i];
            if (bArray[i] != array[i])
            {
                throw new IllegalArgumentException("Value " + array[i]
                        + " cannot be stored in byte array");
            }
        }
        return bArray;
    }

    private short[] toShortArray(byte[] array)
    {
        final short[] sArray = new short[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            sArray[i] = array[i];
        }
        return sArray;
    }

    private short[] toShortArray(int[] array) throws IllegalArgumentException
    {
        final short[] sArray = new short[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            sArray[i] = (short) array[i];
            if (sArray[i] != array[i])
            {
                throw new IllegalArgumentException("Value " + array[i]
                        + " cannot be stored in short array");
            }
        }
        return sArray;
    }

    private int[] toIntArray(byte[] array)
    {
        final int[] iArray = new int[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            iArray[i] = array[i];
        }
        return iArray;
    }

    private int[] toIntArray(short[] array)
    {
        final int[] iArray = new int[array.length];
        for (int i = 0; i < array.length; ++i)
        {
            iArray[i] = array[i];
        }
        return iArray;
    }

    private void checkOrdinalArray(byte[] array) throws IllegalArgumentException
    {
        for (int i = 0; i < array.length; ++i)
        {
            if (array[i] < 0 || array[i] >= type.getValueArray().length)
            {
                throw new IllegalArgumentException("valueIndex " + array[i]
                        + " out of allowed range [0.." + (type.getValueArray().length - 1)
                        + "] of type '" + type.getName() + "'.");
            }
        }
    }

    private void checkOrdinalArray(short[] array) throws IllegalArgumentException
    {
        for (int i = 0; i < array.length; ++i)
        {
            if (array[i] < 0 || array[i] >= type.getValueArray().length)
            {
                throw new IllegalArgumentException("valueIndex " + array[i]
                        + " out of allowed range [0.." + (type.getValueArray().length - 1)
                        + "] of type '" + type.getName() + "'.");
            }
        }
    }

    private void checkOrdinalArray(int[] array) throws IllegalArgumentException
    {
        for (int i = 0; i < array.length; ++i)
        {
            if (array[i] < 0 || array[i] >= type.getValueArray().length)
            {
                throw new IllegalArgumentException("valueIndex " + array[i]
                        + " out of allowed range [0.." + (type.getValueArray().length - 1)
                        + "] of type '" + type.getName() + "'.");
            }
        }
    }

    Object getStorageForm()
    {
        if (bArrayOrNull != null)
        {
            return bArrayOrNull;
        } else if (sArrayOrNull != null)
        {
            return sArrayOrNull;
        } else
        {
            return iArrayOrNull;
        }
    }

    /**
     * Returns the <var>type</var> of this enumeration array.
     */
    public HDF5EnumerationType getType()
    {
        return type;
    }

    /**
     * Returns the number of members of this enumeration array.
     */
    public int getLength()
    {
        return length;
    }

    /**
     * Returns the ordinal value for the <var>arrayIndex</var>.
     * 
     * @param arrayIndex The index in the array to get the ordinal for.
     */
    public int getOrdinal(int arrayIndex)
    {
        if (bArrayOrNull != null)
        {
            return bArrayOrNull[arrayIndex];
        } else if (sArrayOrNull != null)
        {
            return sArrayOrNull[arrayIndex];
        } else
        {
            return iArrayOrNull[arrayIndex];
        }
    }

    /**
     * Returns the string value for <var>arrayIndex</var>.
     * 
     * @param arrayIndex The index in the array to get the value for.
     */
    public String getValue(int arrayIndex)
    {
        return type.getValues().get(getOrdinal(arrayIndex));
    }

    //
    // Iterable
    //

    public Iterator<String> iterator()
    {
        return new Iterator<String>()
            {
                private int index = 0;

                public boolean hasNext()
                {
                    return index < length;
                }

                public String next()
                {
                    return getValue(index++);
                }

                public void remove() throws UnsupportedOperationException
                {
                    throw new UnsupportedOperationException();
                }

            };
    }

}
