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

import java.util.Arrays;
import java.util.BitSet;

/**
 * Contains information about one member of an HDF5 compound data type.
 * 
 * @author Bernd Rinn
 */
public final class HDF5CompoundMemberInformation implements
        Comparable<HDF5CompoundMemberInformation>
{
    private final String memberName;

    private final HDF5DataTypeInformation dataTypeInformation;

    HDF5CompoundMemberInformation(String memberName, HDF5DataTypeInformation dataTypeInformation)
    {
        assert memberName != null;
        assert dataTypeInformation != null;

        this.memberName = memberName;
        this.dataTypeInformation = dataTypeInformation;
    }

    /**
     * Returns the name of the member.
     */
    public String getName()
    {
        return memberName;
    }

    /**
     * Returns the type information of the member with index <var>i</var>.
     */
    public HDF5DataTypeInformation getType()
    {
        return dataTypeInformation;
    }

    /**
     * Creates the compound member information for the given <var>compoundClass</var> and
     * <var>members</var>. The returned array will contain the members in alphabetical order.
     * <p>
     * Can be used to compare compound types, e.g. via
     * {@link java.util.Arrays#equals(Object[], Object[])}.
     */
    public static HDF5CompoundMemberInformation[] create(Class<?> compoundClass,
            final HDF5CompoundMemberMapping... members)
    {
        assert compoundClass != null;
        final HDF5CompoundMemberInformation[] info =
                new HDF5CompoundMemberInformation[members.length];
        for (int i = 0; i < info.length; ++i)
        {
            info[i] =
                    new HDF5CompoundMemberInformation(members[i].getMemberName(),
                            getTypeInformation(compoundClass, members[i]));
        }
        Arrays.sort(info);
        return info;
    }

    private static HDF5DataTypeInformation getTypeInformation(Class<?> compoundClass,
            final HDF5CompoundMemberMapping member)
    {
        final Class<?> fieldType = member.getField(compoundClass).getType();
        final HDF5DataTypeInformation typeInfo;
        if (fieldType == boolean.class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.BOOLEAN, 1);
        } else if (fieldType == byte.class || fieldType == byte[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.INTEGER, 1);
        } else if (fieldType == short.class || fieldType == short[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.INTEGER, 2);
        } else if (fieldType == int.class || fieldType == int[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.INTEGER, 4);
        } else if (fieldType == long.class || fieldType == long[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.INTEGER, 8);
        } else if (fieldType == BitSet.class)
        {
            typeInfo =
                    new HDF5DataTypeInformation(HDF5DataClass.BITFIELD, 8, member
                            .getMemberTypeLength()
                            / 64 + (member.getMemberTypeLength() % 64 != 0 ? 1 : 0));
        } else if (fieldType == float.class || fieldType == float[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.FLOAT, 4);
        } else if (fieldType == double.class || fieldType == double[].class)
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.FLOAT, 8);
        } else if (fieldType == String.class)
        {
            typeInfo =
                    new HDF5DataTypeInformation(HDF5DataClass.STRING, member.getMemberTypeLength());
        } else if (fieldType == HDF5EnumerationValue.class)
        {
            typeInfo =
                    new HDF5DataTypeInformation(HDF5DataClass.ENUM, member.tryGetEnumerationType()
                            .getStorageForm().getStorageSize());
        } else
        {
            typeInfo = new HDF5DataTypeInformation(HDF5DataClass.OTHER, -1);
        }
        if (fieldType.isArray())
        {
            typeInfo.setDimensions(new int[] { member.getMemberTypeLength() });
        }
        return typeInfo;
    }

    //
    // Object
    //

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || obj instanceof HDF5CompoundMemberInformation == false)
        {
            return false;
        }
        final HDF5CompoundMemberInformation that = (HDF5CompoundMemberInformation) obj;
        return memberName.equals(that.memberName)
                && dataTypeInformation.equals(that.dataTypeInformation);
    }

    @Override
    public int hashCode()
    {
        return (17 * 59 + memberName.hashCode()) * 59 + dataTypeInformation.hashCode();
    }

    @Override
    public String toString()
    {
        return memberName + ":" + dataTypeInformation.toString();
    }

    //
    // Comparable<HDF5CompoundMemberInformation>
    //

    public int compareTo(HDF5CompoundMemberInformation o)
    {
        return memberName.compareTo(o.memberName);
    }

}
