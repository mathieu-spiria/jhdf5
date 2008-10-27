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

import ncsa.hdf.hdf5lib.HDFNativeData;

/**
 * A class the represents an HDF enumeration value.
 * 
 * @author Bernd Rinn
 */
public final class HDF5EnumerationValue
{
    private final HDF5EnumerationType type;

    private final int ordinal;

    /**
     * Creates an enumeration value.
     * 
     * @param type The enumeration type of this value.
     * @param ordinal The ordinal value of the value in the <var>type</var>.
     * @throws IllegalArgumentException If the <var>ordinal</var> is outside of the range of allowed
     *             values of the <var>type</var>.
     */
    public HDF5EnumerationValue(HDF5EnumerationType type, int ordinal)
            throws IllegalArgumentException
    {
        assert type != null;

        if (ordinal < 0 || ordinal >= type.getValueArray().length)
        {
            throw new IllegalArgumentException("valueIndex " + ordinal
                    + " out of allowed range [0.." + (type.getValueArray().length - 1)
                    + "] of type '" + type.getName() + "'.");
        }
        this.type = type;
        this.ordinal = ordinal;
    }

    /**
     * Creates an enumeration value.
     * 
     * @param type The enumeration type of this value.
     * @param value The string value (needs to be one of the values of <var>type</var>).
     * @throws IllegalArgumentException If the <var>value</var> is not one of the values of
     *             <var>type</var>.
     */
    public HDF5EnumerationValue(HDF5EnumerationType type, String value)
            throws IllegalArgumentException
    {
        assert type != null;
        assert value != null;

        final Integer valueIndexOrNull = type.tryGetIndexForValue(value);
        if (valueIndexOrNull == null)
        {
            throw new IllegalArgumentException("Value '" + value + "' is not allowed for type '"
                    + type.getName() + "'.");
        }
        this.type = type;
        this.ordinal = valueIndexOrNull;
    }

    /**
     * Returns the <var>type</var> of this enumeration value.
     */
    public HDF5EnumerationType getType()
    {
        return type;
    }

    /**
     * Returns the string value.
     */
    public String getValue()
    {
        return type.getValues().get(ordinal);
    }

    /**
     * Returns the ordinal value.
     */
    public int getOrdinal()
    {
        return ordinal;
    }

    byte[] toStorageForm()
    {
        switch (type.getStorageSize())
        {
            case 1:
                return HDFNativeData.byteToByte((byte) ordinal);
            case 2:
                return HDFNativeData.shortToByte((short) ordinal);
            case 4:
                return HDFNativeData.intToByte(ordinal);
        }
        throw new Error("Illegal storage size.");
    }

    //
    // Object
    //

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ordinal;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        HDF5EnumerationValue other = (HDF5EnumerationValue) obj;
        if (type == null)
        {
            if (other.type != null)
            {
                return false;
            }
        } else if (type.equals(other.type) == false)
        {
            return false;
        }
        if (ordinal != other.ordinal)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return type.getName() + " [" + ordinal + "]";
    }

}
