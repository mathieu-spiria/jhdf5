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

import static ch.systemsx.cisd.hdf5.hdf5lib.H5T.H5Tinsert;

import java.lang.reflect.Field;

import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A class that byteifies member fields of objects.
 * 
 * @author Bernd Rinn
 */
abstract class HDF5MemberByteifyer
{
    private final Field fieldOrNull;

    private final String memberName;

    protected final int size;

    protected final int sizeInBytes;

    protected final int offset;

    protected final CharacterEncoding encoding;

    private final HDF5DataTypeVariant typeVariant;

    HDF5MemberByteifyer(Field fieldOrNull, String memberName, int size, int offset,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        this(fieldOrNull, memberName, size, size, offset, CharacterEncoding.ASCII,
                typeVariantOrNull);
    }

    HDF5MemberByteifyer(Field fieldOrNull, String memberName, int size, int sizeInBytes,
            int offset, CharacterEncoding encoding)
    {
        this(fieldOrNull, memberName, size, sizeInBytes, offset, encoding, HDF5DataTypeVariant.NONE);
    }

    HDF5MemberByteifyer(Field fieldOrNull, String memberName, int size, int sizeInBytes,
            int offset, CharacterEncoding encoding, HDF5DataTypeVariant typeVariantOrNull)
    {
        this.fieldOrNull = fieldOrNull;
        this.memberName = memberName;
        this.size = size;
        this.sizeInBytes = sizeInBytes;
        this.offset = offset;
        this.encoding = encoding;
        this.typeVariant = HDF5DataTypeVariant.maskNull(typeVariantOrNull);
    }

    public abstract byte[] byteify(int compoundDataTypeId, Object obj)
            throws IllegalAccessException;

    public abstract void setFromByteArray(int compoundDataTypeId, Object obj, byte[] byteArr,
            int arrayOffset) throws IllegalAccessException;

    protected abstract int getMemberStorageTypeId();

    /**
     * Returns -1 if the native type id should be inferred from the storage type id
     */
    protected abstract int getMemberNativeTypeId();

    public void insertType(int dataTypeId)
    {
        H5Tinsert(dataTypeId, memberName, offset, getMemberStorageTypeId());
    }

    public void insertNativeType(int dataTypeId, HDF5 h5, ICleanUpRegistry registry)
    {
        if (getMemberNativeTypeId() < 0)
        {
            H5Tinsert(dataTypeId, memberName, offset,
                    h5.getNativeDataType(getMemberStorageTypeId(), registry));
        } else
        {
            H5Tinsert(dataTypeId, memberName, offset, getMemberNativeTypeId());
        }
    }

    public String getMemberName()
    {
        return memberName;
    }
    
    Field tryGetField()
    {
        return fieldOrNull;
    }

    public int getSize()
    {
        return size;
    }

    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    public int getOffset()
    {
        return offset;
    }

    public int getTotalSize()
    {
        return offset + sizeInBytes;
    }

    public HDF5DataTypeVariant getTypeVariant()
    {
        return typeVariant;
    }

    public String describe()
    {
        if (fieldOrNull != null)
        {
            return "field '" + fieldOrNull.getName() + "' of class '"
                    + fieldOrNull.getDeclaringClass().getCanonicalName() + "'";
        } else
        {
            return "member '" + memberName + "'";
        }
    }
    
    public boolean isDummy()
    {
        return false;
    }
    
    public boolean mayBeCut()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return describe();
    }
}
