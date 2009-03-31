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

import java.lang.reflect.Field;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * A class that maps a Java field to a member of a HDF5 compound data type.
 * <p>
 * Example on how to use:
 * 
 * <pre>
 * static class Record
 *     {
 *         int i;
 * 
 *         String s;
 * 
 *         HDF5EnumerationValue e;
 * 
 *         Record(int i, String s, HDF5EnumerationValue e)
 *         {
 *             this.i = i;
 *             this.e = e;
 *             this.s = s;
 *         }
 * 
 *         Record()
 *         {
 *         }
 * 
 *         static HDF5CompoundType&lt;Record&gt; getHDF5Type(HDF5Reader reader)
 *         {
 *             final HDF5EnumerationType enumType = reader.getEnumType(&quot;someEnumType&quot;, new String[]
 *                 { &quot;1&quot;, &quot;Two&quot;, &quot;THREE&quot; });
 *             return reader.getCompoundType(Record.class, cpdm(&quot;i&quot;), 
 *                      cpdm(&quot;s&quot;, 20), cpdm(&quot;e&quot;, enumType));
 *         }
 * 
 *     }
 *         
 *     ...
 *         
 *     final HDF5Writer writer = new HDF5Writer(new File(&quot;test.h5&quot;).open();
 *     final HDF5CompoundType&lt;Record&gt; compoundType = Record.getHDF5Type(writer);
 *     final HDF5EnumerationType enumType = writer.getEnumType(&quot;someEnumType&quot;);
 *     Record[] array =
 *             new Record[]
 *                 {
 *                         new Record(1, &quot;some text&quot;,
 *                                 new HDF5EnumerationValue(enumType, &quot;THREE&quot;)),
 *                         new Record(2, &quot;some note&quot;,
 *                                 new HDF5EnumerationValue(enumType, &quot;1&quot;)), };
 *     writer.writeCompound(&quot;/testCompound&quot;, compoundType, recordWritten);
 *     writer.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
public final class HDF5CompoundMemberMapping
{

    private final String fieldName;

    private final String memberName;

    private final int memberTypeLength;

    private final HDF5EnumerationType enumTypeOrNull;

    /**
     * Adds a member mapping for <var>fieldName</var>. Can be used for all data types except Strings
     * and Enumerations.
     * 
     * @param fieldName The name of the field in the Java class. Will also be used as name of
     *            member.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName)
    {
        return new HDF5CompoundMemberMapping(fieldName, fieldName, null, 0);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Can be used for all data types except Strings
     * and Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName)
    {
        return new HDF5CompoundMemberMapping(fieldName, memberName, null, 0);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Strings, primitive arrays.
     * and {@link java.util.BitSet}s.
     * 
     * @param fieldName The name of the field in the Java class. Will also be used as name of
     *            member.
     * @param memberTypeLength The length of the String or the primitive array in the compound type.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName, int memberTypeLength)
    {
        return new HDF5CompoundMemberMapping(fieldName, fieldName, null, memberTypeLength);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Strings, primitive arrays.
     * and {@link java.util.BitSet}s.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param memberTypeLength The length of the String or the primitive array in the compound type.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            int memberTypeLength)
    {
        return new HDF5CompoundMemberMapping(fieldName, memberName, null, memberTypeLength);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param enumType The enumeration type in the HDF5 file.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName, HDF5EnumerationType enumType)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, fieldName, enumType, 0);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param enumType The enumeration type in the HDF5 file.
     */
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            HDF5EnumerationType enumType)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, memberName, enumType, 0);
    }

    /**
     * A {@link HDF5CompoundMemberMapping} that allows to provide an explicit <var>memberName</var>
     * that differs from the <var>fieldName</var> and the maximal length in case of a String member.
     * 
     * @param fieldName The name of the field in the <var>clazz</var>
     * @param memberName The name of the member in the HDF5 compound data type.
     * @param memberTypeLength The length of the String member field (or 0, if no String member).
     */
    private HDF5CompoundMemberMapping(String fieldName, String memberName,
            HDF5EnumerationType enumTypeOrNull, int memberTypeLength)
    {
        this.fieldName = fieldName;
        this.memberName = memberName;
        this.enumTypeOrNull = enumTypeOrNull;
        this.memberTypeLength = memberTypeLength;
    }

    Field getField(Class<?> clazz) throws HDF5JavaException
    {
        try
        {
            final Field field = clazz.getDeclaredField(fieldName);
            if (memberTypeLength > 0 && field.getType() != String.class
                    && field.getType().isArray() == false
                    && field.getType() != java.util.BitSet.class)
            {
                throw new HDF5JavaException("Field '" + fieldName + "' of class '"
                        + clazz.getCanonicalName()
                        + "' is no String or primitive array, but a length > 0 is given.");
            } else if (memberTypeLength == 0
                    && (field.getType() == String.class || field.getType().isArray() || field
                            .getType() == java.util.BitSet.class))
            {
                throw new HDF5JavaException("Field '" + fieldName + "' of class '"
                        + clazz.getCanonicalName()
                        + "' is a String or primitive array, but a length == 0 is given.");
            }
            return field;
        } catch (NoSuchFieldException ex)
        {
            throw new HDF5JavaException("No field '" + fieldName + "' found for class '"
                    + clazz.getCanonicalName() + "'.");
        }
    }

    String getMemberName()
    {
        return memberName;
    }

    int getMemberTypeLength()
    {
        return memberTypeLength;
    }

    HDF5EnumerationType tryGetEnumerationType()
    {
        return enumTypeOrNull;
    }
}
