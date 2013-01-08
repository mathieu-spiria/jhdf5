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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A class that byteifies Java value objects. The fields have to be specified by name. This class
 * can handle all primitive types and Strings.
 * 
 * @author Bernd Rinn
 */
class HDF5ValueObjectByteifyer<T>
{

    private final HDF5MemberByteifyer[] byteifyers;

    private final int recordSize;

    private Class<?> cachedRecordClass;

    private Constructor<?> cachedDefaultConstructor;

    @SuppressWarnings("unchecked")
    private static <T> T newMap(int size)
    {
        return (T) new HDF5CompoundDataMap(size);
    }

    @SuppressWarnings("unchecked")
    private static <T> T newList(int size)
    {
        return (T) new HDF5CompoundDataList(Collections.nCopies(size, null));
    }

    @SuppressWarnings("unchecked")
    private static <T> T newArray(int size)
    {
        return (T) new Object[size];
    }

    /** A role that provides some information about the HDF5 file to this byteifyer. */
    interface FileInfoProvider
    {
        public int getBooleanDataTypeId();

        public int getStringDataTypeId(int maxLength);

        public int getArrayTypeId(int baseTypeId, int length);

        public int getArrayTypeId(int baseTypeId, int[] dimensions);
        
        public HDF5EnumerationType getEnumType(String[] options);

        public CharacterEncoding getCharacterEncoding(int dataTypeId);
    }

    HDF5ValueObjectByteifyer(Class<T> clazz, FileInfoProvider fileInfoProvider,
            CompoundTypeInformation compoundTypeInfoOrNull, HDF5CompoundMemberMapping... members)
    {
        byteifyers =
                HDF5CompoundByteifyerFactory.createMemberByteifyers(clazz, fileInfoProvider,
                        compoundTypeInfoOrNull, members);
        if (compoundTypeInfoOrNull != null)
        {
            recordSize = compoundTypeInfoOrNull.recordSize;
        } else if (byteifyers.length > 0)
        {
            recordSize = byteifyers[byteifyers.length - 1].getTotalSize();
        } else
        {
            recordSize = 0;
        }
    }

    public int insertMemberTypes(int dataTypeId)
    {
        for (HDF5MemberByteifyer byteifyer : byteifyers)
        {
            byteifyer.insertType(dataTypeId);
        }
        return dataTypeId;
    }

    public int insertNativeMemberTypes(int dataTypeId, HDF5 h5, ICleanUpRegistry registry)
    {
        for (HDF5MemberByteifyer byteifyer : byteifyers)
        {
            byteifyer.insertNativeType(dataTypeId, h5, registry);
        }
        return dataTypeId;
    }

    /**
     * @throw {@link HDF5JavaException} if one of the elements in <var>arr</var> exceeding its
     *        pre-defined size.
     */
    public byte[] byteify(int compoundDataTypeId, T[] arr) throws HDF5JavaException
    {
        final byte[] barray = new byte[arr.length * recordSize];
        int offset = 0;
        int counter = 0;
        for (Object obj : arr)
        {
            for (HDF5MemberByteifyer byteifyer : byteifyers)
            {
                try
                {
                    final byte[] b = byteifyer.byteify(compoundDataTypeId, obj);
                    if (b.length > byteifyer.getSizeInBytes())
                    {
                        throw new HDF5JavaException("Compound " + byteifyer.describe()
                                + " of array element " + counter + " must not exceed "
                                + byteifyer.getSizeInBytes() + " bytes, but is of size " + b.length
                                + " bytes.");
                    }
                    System.arraycopy(b, 0, barray, offset + byteifyer.getOffset(), b.length);
                } catch (IllegalAccessException ex)
                {
                    throw new HDF5JavaException("Error accessing " + byteifyer.describe());
                }
            }
            offset += recordSize;
            ++counter;
        }
        return barray;
    }

    /**
     * @throw {@link HDF5JavaException} if <var>obj</var> exceeding its pre-defined size.
     */
    public byte[] byteify(int compoundDataTypeId, T obj) throws HDF5JavaException
    {
        final byte[] barray = new byte[recordSize];
        for (HDF5MemberByteifyer byteifyer : byteifyers)
        {
            try
            {
                final byte[] b = byteifyer.byteify(compoundDataTypeId, obj);
                if (b.length > byteifyer.getSizeInBytes())
                {
                    throw new HDF5JavaException("Compound " + byteifyer.describe()
                            + " must not exceed " + byteifyer.getSizeInBytes()
                            + " bytes, but is of size " + b.length + " bytes.");
                }
                System.arraycopy(b, 0, barray, byteifyer.getOffset(), b.length);
            } catch (IllegalAccessException ex)
            {
                throw new HDF5JavaException("Error accessing " + byteifyer.describe());
            }
        }
        return barray;
    }

    public T[] arrayify(int compoundDataTypeId, byte[] byteArr, Class<T> recordClass)
    {
        final int length = byteArr.length / recordSize;
        if (length * recordSize != byteArr.length)
        {
            throw new HDF5JavaException("Illegal byte array for compound type (length "
                    + byteArr.length + " is not a multiple of record size " + recordSize + ")");
        }
        final T[] result = HDF5Utils.createArray(recordClass, length);
        int offset = 0;
        for (int i = 0; i < length; ++i)
        {
            result[i] = primArrayifyScalar(compoundDataTypeId, byteArr, recordClass, offset);
            offset += recordSize;
        }
        return result;
    }

    public T arrayifyScalar(int compoundDataTypeId, byte[] byteArr, Class<T> recordClass)
    {
        if (byteArr.length < recordSize)
        {
            throw new HDF5JavaException("Illegal byte array for scalar compound type (length "
                    + byteArr.length + " is smaller than record size " + recordSize + ")");
        }
        return primArrayifyScalar(compoundDataTypeId, byteArr, recordClass, 0);
    }

    private T primArrayifyScalar(int compoundDataTypeId, byte[] byteArr, Class<T> recordClass,
            int offset)
    {
        T result = newInstance(recordClass);
        for (HDF5MemberByteifyer byteifyer : byteifyers)
        {
            try
            {
                byteifyer.setFromByteArray(compoundDataTypeId, result, byteArr, offset);
            } catch (IllegalAccessException ex)
            {
                throw new HDF5JavaException("Error accessing " + byteifyer.describe());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private T newInstance(Class<?> recordClass) throws HDF5JavaException
    {
        if (Map.class.isAssignableFrom(recordClass))
        {
            return newMap(byteifyers.length);
        }
        if (List.class.isAssignableFrom(recordClass))
        {
            return newList(byteifyers.length);
        }
        if (recordClass == Object[].class)
        {
            return newArray(byteifyers.length);
        }
        try
        {
            if (recordClass != cachedRecordClass)
            {
                cachedRecordClass = recordClass;
                cachedDefaultConstructor = ReflectionUtils.getDefaultConstructor(recordClass);
            }
            return (T) cachedDefaultConstructor.newInstance();
        } catch (Exception ex)
        {
            throw new HDF5JavaException("Creation of new object of class "
                    + recordClass.getCanonicalName() + " by default constructor failed: "
                    + ex.toString());
        }
    }

    public int getRecordSize()
    {
        return recordSize;
    }

    public HDF5MemberByteifyer[] getByteifyers()
    {
        return byteifyers;
    }

    /**
     * Returns <code>true</code> if the value object byteifyer has any members that cannot be mapped
     * to the in-memory representation.
     */
    public boolean hasUnmappedMembers()
    {
        for (HDF5MemberByteifyer memberByteifyer : byteifyers)
        {
            if (memberByteifyer.isDummy())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a list with the names of all members that cannot be mapped to the in-memory
     * representation.
     */
    public String[] getUnmappedMembers()
    {
        if (hasUnmappedMembers())
        {
            final List<String> unmappedMembers = new ArrayList<String>();
            for (HDF5MemberByteifyer memberByteifyer : byteifyers)
            {
                if (memberByteifyer.isDummy())
                {
                    unmappedMembers.add(memberByteifyer.getMemberName());
                }
            }
            return unmappedMembers.toArray(new String[unmappedMembers.size()]);
        } else
        {
            return new String[0];
        }
    }

    //
    // Object
    //

    @Override
    public String toString()
    {
        return "HDF5ValueObjectByteifyer [byteifyers=" + Arrays.toString(byteifyers) + "]";
    }

}
