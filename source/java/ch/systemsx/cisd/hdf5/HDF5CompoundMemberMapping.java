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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import ch.systemsx.cisd.base.mdarray.MDAbstractArray;
import ch.systemsx.cisd.base.mdarray.MDArray;

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
 *             return reader.getCompoundType(Record.class, mapping(&quot;i&quot;), 
 *                      mapping(&quot;s&quot;, 20), mapping(&quot;e&quot;, enumType));
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
 * A simpler form is to let JHDF5 infer the mapping between fields in the Java object and members of
 * the compound data type, see {@link #inferMapping(Class)} and {@link #inferMapping(Class, Map)}
 * <p>
 * The following Java types can be mapped to compound members:
 * <ul>
 * <li>Primitive values</li>
 * <li>Primitive arrays</li>
 * <li>Primitive matrices (except <code>char[][]</code>)</li>
 * <li>{@link String}</li>
 * <li>{@link java.util.BitSet}</li>
 * <li>{@link java.util.Date}</li>
 * <li>{@link HDF5EnumerationValue}</li>
 * <li>{@link HDF5EnumerationValueArray}</li>
 * <li>Sub-classes of {@link MDAbstractArray}</li>
 * </ul>
 * 
 * @author Bernd Rinn
 */
public final class HDF5CompoundMemberMapping
{

    private final String memberName;

    private final int storageDataTypeId;

    private String fieldName;

    private Class<?> memberClassOrNull;

    private int memberTypeLength;

    private int[] memberTypeDimensions;

    private HDF5EnumerationType enumTypeOrNull;

    private HDF5DataTypeVariant typeVariantOrNull;

    /**
     * Adds a member mapping for <var>memberName</var>.
     * 
     * @param memberName The name of the member in the compound type. field in the Java class. Will
     *            also be used to find the name of the field in the Java class if not overridden by
     *            {@link #fieldName(String)}.
     */
    public static HDF5CompoundMemberMapping mapping(String memberName)
    {
        return new HDF5CompoundMemberMapping(memberName, null, memberName, null, new int[0]);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Can be used for all data types except Strings
     * and Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @deprecated Use {@link #mapping(String)} and {@link #fieldName(String)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, memberName, null, new int[0]);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Strings, primitive arrays
     * and {@link java.util.BitSet}s.
     * 
     * @param fieldName The name of the field in the Java class. Will also be used as name of
     *            member.
     * @param memberTypeLength The length of the String or the primitive array in the compound type.
     * @deprecated Use {@link #mapping(String)} and {@link #length(int)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, int memberTypeLength)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, fieldName, null, new int[]
            { memberTypeLength });
    }

    /**
     * Adds a member mapping for <var>fieldName</var>.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param memberClass The class of the member. Only used if the compound pojo class is a map.
     *            For restrictions on the type, see above.
     * @param memberDimensions The dimensions of the compound type (i.e. length of the String or
     *            dimensions of the array).
     * @param storageDataTypeId The storage data type id of the member, if known, or -1 else
     * @param typeVariantOrNull The data type variant of this mapping or <code>null</code>
     */
    static HDF5CompoundMemberMapping mappingArrayWithStorageId(String fieldName, String memberName,
            Class<?> memberClass, int[] memberDimensions, int storageDataTypeId,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        return new HDF5CompoundMemberMapping(fieldName, memberClass, memberName, null,
                memberDimensions, storageDataTypeId, typeVariantOrNull);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Strings, primitive arrays.
     * and {@link java.util.BitSet}s.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param memberTypeLength The length of the String or the primitive array in the compound type.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)} and {@link #length(int)}
     *             instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            int memberTypeLength)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, memberName, null, new int[]
            { memberTypeLength });
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for two-dimensional primitive
     * arrays or {@link MDArray}s.
     * 
     * @param fieldName The name of the field in the Java class. Will also be used as name of
     *            member.
     * @param memberTypeDimX The x dimension of the array in the compound type.
     * @param memberTypeDimY The y dimension of the array in the compound type.
     * @deprecated Use {@link #mapping(String)} and {@link #dimensions(int, int)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, int memberTypeDimX,
            int memberTypeDimY)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, fieldName, null, new int[]
            { memberTypeDimX, memberTypeDimY });
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for two-dimensional primitive
     * arrays or {@link MDArray}s.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param memberTypeDimX The x dimension of the array in the compound type.
     * @param memberTypeDimY The y dimension of the array in the compound type.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)} and
     *             {@link #dimensions(int, int)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            int memberTypeDimX, int memberTypeDimY)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, memberName, null, new int[]
            { memberTypeDimX, memberTypeDimY });
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for two-dimensional primitive
     * arrays or {@link MDArray}s.
     * 
     * @param fieldName The name of the field in the Java class. Will also be used as name of
     *            member.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @deprecated Use {@link #mapping(String)} and {@link #dimensions(int[])} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, int[] memberTypeDimensions)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, fieldName, null, memberTypeDimensions);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for two-dimensional primitive
     * arrays or {@link MDArray}s.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)} and
     *             {@link #dimensions(int[])} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            int[] memberTypeDimensions)
    {
        return new HDF5CompoundMemberMapping(fieldName, null, memberName, null,
                memberTypeDimensions);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param enumType The enumeration type in the HDF5 file.
     * @deprecated Use {@link #mapping(String)} and {@link #enumType(HDF5EnumerationType)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, HDF5EnumerationType enumType)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValue.class, fieldName,
                enumType, new int[0]);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumeration arrays.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param enumType The enumeration type in the HDF5 file.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)} and
     *             {@link #enumType(HDF5EnumerationType)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, HDF5EnumerationType enumType,
            int[] memberTypeDimensions)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValueArray.class, fieldName,
                enumType, memberTypeDimensions);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumeration arrays.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param enumType The enumeration type in the HDF5 file.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)},
     *             {@link #dimensions(int[])} and {@link #enumType(HDF5EnumerationType)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            HDF5EnumerationType enumType, int[] memberTypeDimensions)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValueArray.class,
                memberName, enumType, memberTypeDimensions);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumeration arrays.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param enumType The enumeration type in the HDF5 file.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @param storageTypeId the id of the storage type of this member.
     */
    static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            HDF5EnumerationType enumType, int[] memberTypeDimensions, int storageTypeId)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValueArray.class,
                memberName, enumType, memberTypeDimensions, storageTypeId, null);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumeration arrays.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param enumType The enumeration type in the HDF5 file.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @param storageTypeId the id of the storage type of this member.
     * @param typeVariantOrNull The data type variant of this mapping or <code>null</code>
     */
    static HDF5CompoundMemberMapping mappingWithStorageTypeId(String fieldName, String memberName,
            HDF5EnumerationType enumType, int[] memberTypeDimensions, int storageTypeId,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValueArray.class,
                memberName, enumType, memberTypeDimensions, storageTypeId, typeVariantOrNull);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumeration arrays.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberClass The class of the member. For restrictions on the type, see above.
     * @param enumType The enumeration type in the HDF5 file.
     * @param memberTypeDimensions The dimensions of the array in the compound type.
     * @param storageTypeId the id of the storage type of this member.
     */
    static HDF5CompoundMemberMapping mapping(String fieldName, Class<?> memberClass,
            HDF5EnumerationType enumType, int[] memberTypeDimensions, int storageTypeId)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, memberClass, fieldName, enumType,
                memberTypeDimensions, storageTypeId, null);
    }

    /**
     * Adds a member mapping for <var>fieldName</var>. Only suitable for Enumerations.
     * 
     * @param fieldName The name of the field in the Java class.
     * @param memberName The name of the member in the compound type.
     * @param enumType The enumeration type in the HDF5 file.
     * @deprecated Use {@link #mapping(String)}, {@link #fieldName(String)} and
     *             {@link #enumType(HDF5EnumerationType)} instead.
     */
    @Deprecated
    public static HDF5CompoundMemberMapping mapping(String fieldName, String memberName,
            HDF5EnumerationType enumType)
    {
        assert enumType != null;
        return new HDF5CompoundMemberMapping(fieldName, HDF5EnumerationValue.class, memberName,
                enumType, new int[0]);
    }

    /**
     * Returns the inferred compound member mapping for the given <var>pojoClass</var>. This method
     * honors the annotations {@link CompoundType} and {@link CompoundElement}.
     * <p>
     * <em>Note 1:</em> All fields that correspond to members with a variable length (e.g. Strings,
     * primitive arrays and matrices and objects of type <code>MDXXXArray</code>) need to be
     * annotated with {@link CompoundElement} specifying their dimensions using
     * {@link CompoundElement#dimensions()}. .
     * <p>
     * <em>Note 2:</em> <var>pojoClass</var> containing HDF5 enumerations cannot have their mapping
     * inferred as the HDF5 enumeration type needs to be explicitly specified in the mapping.
     * <p>
     * <em>Example 1:</em>
     * 
     * <pre>
     * class Record1
     * {
     *     &#064;CompoundElement(dimension = 10)
     *     String s;
     * 
     *     float f;
     * }
     * </pre>
     * 
     * will lead to:
     * 
     * <pre>
     * inferMapping(Record1.class) -> { mapping("s", 10), mapping("f") }
     * </pre>
     * 
     * <em>Example 2:</em>
     * 
     * <pre>
     * &#064;CompoundType(mapAllFields = false)
     * class Record2
     * {
     *     &#064;CompoundElement(memberName = &quot;someString&quot;, dimension = 10)
     *     String s;
     * 
     *     float f;
     * }
     * </pre>
     * 
     * will lead to:
     * 
     * <pre>
     * inferMapping(Record2.class) -> { mapping("s", "someString", 10) }
     * </pre>
     */
    public static HDF5CompoundMemberMapping[] inferMapping(final Class<?> pojoClass)
    {
        return inferMapping(pojoClass, null);
    }

    /**
     * Returns the inferred compound member mapping for the given <var>pojoClass</var>. This method
     * honors the annotations {@link CompoundType} and {@link CompoundElement}.
     * <p>
     * <em>Note 1:</em> All fields that correspond to members with a variable length (e.g. Strings,
     * primitive arrays and matrices and objects of type <code>MDXXXArray</code>) need to be
     * annotated with {@link CompoundElement} specifying their dimensions using
     * {@link CompoundElement#dimensions()}. .
     * <p>
     * <em>Note 2:</em> <var>pojoClass</var> containing HDF5 enumerations need to have their
     * {@link HDF5EnumerationType} specified in the <var>fieldNameToEnumTypeMapOrNull</var>. You may
     * use {@link #inferEnumerationTypeMap(Object)} to create
     * <var>fieldNameToEnumTypeMapOrNull</var>.
     * <p>
     * <em>Example 1:</em>
     * 
     * <pre>
     * class Record1
     * {
     *     &#064;CompoundElement(dimension = 10)
     *     String s;
     * 
     *     float f;
     * }
     * </pre>
     * 
     * will lead to:
     * 
     * <pre>
     * inferMapping(Record1.class) -> { mapping("s", 10), mapping("f") }
     * </pre>
     * 
     * <em>Example 2:</em>
     * 
     * <pre>
     * &#064;CompoundType(mapAllFields = false)
     * class Record2
     * {
     *     &#064;CompoundElement(memberName = &quot;someString&quot;, dimension = 10)
     *     String s;
     * 
     *     float f;
     * }
     * </pre>
     * 
     * will lead to:
     * 
     * <pre>
     * inferMapping(Record2.class) -> { mapping("s", "someString", 10) }
     * </pre>
     */
    public static HDF5CompoundMemberMapping[] inferMapping(final Class<?> pojoClass,
            final Map<String, HDF5EnumerationType> fieldNameToEnumTypeMapOrNull)
    {
        final List<HDF5CompoundMemberMapping> result =
                new ArrayList<HDF5CompoundMemberMapping>(pojoClass.getDeclaredFields().length);
        final CompoundType ct = pojoClass.getAnnotation(CompoundType.class);
        final boolean includeAllFields = (ct != null) ? ct.mapAllFields() : true;
        for (Class<?> c = pojoClass; c != null; c = c.getSuperclass())
        {
            for (Field f : c.getDeclaredFields())
            {
                final HDF5EnumerationType enumTypeOrNull =
                        (fieldNameToEnumTypeMapOrNull != null) ? fieldNameToEnumTypeMapOrNull.get(f
                                .getName()) : null;
                final CompoundElement e = f.getAnnotation(CompoundElement.class);
                if (e != null)
                {
                    result.add(new HDF5CompoundMemberMapping(f.getName(), null, StringUtils
                            .defaultIfEmpty(e.memberName(), f.getName()), enumTypeOrNull, e
                            .dimensions(), HDF5DataTypeVariant.unmaskNone(e.typeVariant())));
                } else if (includeAllFields)
                {
                    result.add(new HDF5CompoundMemberMapping(f.getName(), null, f.getName(),
                            enumTypeOrNull, new int[0]));
                }
            }
        }
        return result.toArray(new HDF5CompoundMemberMapping[result.size()]);
    }

    /**
     * Infers the map from field names to {@link HDF5EnumerationType}s for the given <var>pojo</var>
     * object.
     */
    public static <T> Map<String, HDF5EnumerationType> inferEnumerationTypeMap(T pojo)
    {
        Map<String, HDF5EnumerationType> resultOrNull = null;
        for (Class<?> c = pojo.getClass(); c != null; c = c.getSuperclass())
        {
            for (Field f : c.getDeclaredFields())
            {
                if (f.getType() == HDF5EnumerationValue.class)
                {
                    ReflectionUtils.ensureAccessible(f);
                    try
                    {
                        if (resultOrNull == null)
                        {
                            resultOrNull = new HashMap<String, HDF5EnumerationType>();
                        }
                        resultOrNull.put(f.getName(),
                                ((HDF5EnumerationValue) f.get(pojo)).getType());
                    } catch (IllegalArgumentException ex)
                    {
                        throw new Error(ex);
                    } catch (IllegalAccessException ex)
                    {
                        throw new Error(ex);
                    }
                }
                if (f.getType() == HDF5EnumerationValueArray.class)
                {
                    ReflectionUtils.ensureAccessible(f);
                    try
                    {
                        if (resultOrNull == null)
                        {
                            resultOrNull = new HashMap<String, HDF5EnumerationType>();
                        }
                        resultOrNull.put(f.getName(),
                                ((HDF5EnumerationValueArray) f.get(pojo)).getType());
                    } catch (IllegalArgumentException ex)
                    {
                        throw new Error(ex);
                    } catch (IllegalAccessException ex)
                    {
                        throw new Error(ex);
                    }
                }
            }
        }
        return resultOrNull;
    }

    @SuppressWarnings("rawtypes")
    private final static IdentityHashMap<Class, HDF5DataTypeVariant> typeVariantMap =
            new IdentityHashMap<Class, HDF5DataTypeVariant>();

    static
    {
        typeVariantMap.put(java.util.Date.class,
                HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH);
    }

    /**
     * A {@link HDF5CompoundMemberMapping} that allows to provide an explicit <var>memberName</var>
     * that differs from the <var>fieldName</var> and the maximal length in case of a String member.
     * 
     * @param fieldName The name of the field in the <var>clazz</var>
     * @param memberClassOrNull The class of the member, if a map is used as the compound pojo.
     * @param memberName The name of the member in the HDF5 compound data type.
     * @param memberTypeDimensions The dimensions of the member type, or 0 for a scalar value.
     */
    private HDF5CompoundMemberMapping(String fieldName, Class<?> memberClassOrNull,
            String memberName, HDF5EnumerationType enumTypeOrNull, int[] memberTypeDimensions)
    {
        this(fieldName, memberClassOrNull, memberName, enumTypeOrNull, memberTypeDimensions, -1,
                null);
    }

    /**
     * A {@link HDF5CompoundMemberMapping} that allows to provide an explicit <var>memberName</var>
     * that differs from the <var>fieldName</var> and the maximal length in case of a String member.
     * 
     * @param fieldName The name of the field in the <var>clazz</var>
     * @param memberClassOrNull The class of the member, if a map is used as the compound pojo.
     * @param memberName The name of the member in the HDF5 compound data type.
     * @param memberTypeDimensions The dimensions of the member type, or 0 for a scalar value.
     * @param typeVariantOrNull The data type variant of this mapping, or <code>null</code> if this
     *            mapping has no type variant.
     */
    private HDF5CompoundMemberMapping(String fieldName, Class<?> memberClassOrNull,
            String memberName, HDF5EnumerationType enumTypeOrNull, int[] memberTypeDimensions,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        this(fieldName, memberClassOrNull, memberName, enumTypeOrNull, memberTypeDimensions, -1,
                typeVariantOrNull);
    }

    /**
     * A {@link HDF5CompoundMemberMapping} that allows to provide an explicit <var>memberName</var>
     * that differs from the <var>fieldName</var> and the maximal length in case of a String member.
     * 
     * @param fieldName The name of the field in the <var>clazz</var>
     * @param memberClassOrNull The class of the member, if a map is used as the compound pojo.
     * @param memberName The name of the member in the HDF5 compound data type.
     * @param memberTypeDimensions The dimensions of the member type, or 0 for a scalar value.
     * @param storageMemberTypeId The storage data type id of member, or -1, if not available
     */
    private HDF5CompoundMemberMapping(String fieldName, Class<?> memberClassOrNull,
            String memberName, HDF5EnumerationType enumTypeOrNull, int[] memberTypeDimensions,
            int storageMemberTypeId, HDF5DataTypeVariant typeVariantOrNull)
    {
        this.fieldName = fieldName;
        this.memberClassOrNull = memberClassOrNull;
        this.memberName = memberName;
        this.enumTypeOrNull = enumTypeOrNull;
        this.memberTypeDimensions = memberTypeDimensions;
        this.memberTypeLength = MDArray.getLength(memberTypeDimensions);
        this.storageDataTypeId = storageMemberTypeId;
        if (typeVariantMap.containsKey(typeVariantOrNull))
        {
            this.typeVariantOrNull = typeVariantMap.get(typeVariantOrNull);
        } else
        {
            this.typeVariantOrNull = HDF5DataTypeVariant.maskNull(typeVariantOrNull);
        }
    }

    /**
     * Sets the field name in the Java class to use for the mapping, overriding the member name
     * which is used by default to find the field.
     */
    @SuppressWarnings("hiding")
    public HDF5CompoundMemberMapping fieldName(String fieldName)
    {
        this.fieldName = fieldName;
        return this;
    }

    Field getField(Class<?> clazz) throws HDF5JavaException
    {
        return getField(clazz, clazz);
    }

    private Field getField(Class<?> clazz, Class<?> searchClass) throws HDF5JavaException
    {
        try
        {
            final Field field = clazz.getDeclaredField(fieldName);
            final Class<?> fieldType = field.getType();
            final boolean isArray = fieldType.isArray();
            final boolean isMDArray = MDAbstractArray.class.isAssignableFrom(fieldType);
            if (memberTypeLength > 1)
            {

                if (field.getType() != String.class && isArray == false
                        && field.getType() != java.util.BitSet.class
                        && field.getType() != HDF5EnumerationValueArray.class && isMDArray == false)
                {
                    throw new HDF5JavaException("Field '" + fieldName + "' of class '"
                            + clazz.getCanonicalName()
                            + "' is no String or array, but a length > 1 is given.");
                }

            } else if (memberTypeLength == 0
                    && (field.getType() == String.class || isArray || isMDArray || field.getType() == java.util.BitSet.class))
            {
                throw new HDF5JavaException("Field '" + fieldName + "' of class '"
                        + clazz.getCanonicalName()
                        + "' is a String or array, but a length == 0 is given.");
            }
            return field;
        } catch (NoSuchFieldException ex)
        {
            final Class<?> superClassOrNull = clazz.getSuperclass();
            if (superClassOrNull == null || superClassOrNull == Object.class)
            {
                throw new HDF5JavaException("No field '" + fieldName + "' found for class '"
                        + searchClass.getCanonicalName() + "'.");
            } else
            {
                return getField(superClassOrNull, searchClass);
            }
        }
    }

    String getMemberName()
    {
        return memberName;
    }

    /**
     * Sets the member class to use for the mapping.
     */
    public HDF5CompoundMemberMapping memberClass(Class<?> memberClass)
    {
        this.memberClassOrNull = memberClass;
        return this;
    }

    public Class<?> tryGetMemberClass()
    {
        return memberClassOrNull;
    }

    /**
     * Sets the length of the member type to use for the mapping. Must be set for String, BitSet.
     * Can be used as a convenience method replacing {@link #dimensions(int[])} for array members of
     * rank 1.
     */
    @SuppressWarnings("hiding")
    public HDF5CompoundMemberMapping length(int memberTypeLength)
    {
        return dimensions(new int[]
            { memberTypeLength });
    }

    int getMemberTypeLength()
    {
        return memberTypeLength;
    }

    /**
     * Sets the dimensions of the member type to use for the mapping. Convenience method replacing
     * {@link #dimensions(int[])} for array members of rank 2.
     */
    public HDF5CompoundMemberMapping dimensions(int memberTypeDimensionX, int memberTypeDimensionY)
    {
        this.memberTypeDimensions = new int[]
            { memberTypeDimensionX, memberTypeDimensionY };
        this.memberTypeLength = MDArray.getLength(memberTypeDimensions);
        return this;
    }

    /**
     * Sets the dimensions of the member type to use for the mapping. Must be set for array members
     * of rank N.
     */
    @SuppressWarnings("hiding")
    public HDF5CompoundMemberMapping dimensions(int[] memberTypeDimensions)
    {
        this.memberTypeDimensions = memberTypeDimensions;
        this.memberTypeLength = MDArray.getLength(memberTypeDimensions);
        if (enumTypeOrNull != null)
        {
            checkEnumArrayRank();
            this.memberClassOrNull = HDF5EnumerationValueArray.class;
        }
        return this;
    }

    private void checkEnumArrayRank()
    {
        if (memberTypeDimensions != null && memberTypeDimensions.length > 1)
        {
            throw new HDF5JavaException("Enumeration arrays only supported with rank 1 [rank="
                    + memberTypeDimensions.length + "]");
        }
    }

    int[] getMemberTypeDimensions()
    {
        return memberTypeDimensions;
    }

    int getStorageDataTypeId()
    {
        return storageDataTypeId;
    }

    /**
     * Sets the enumeration type to use for the mapping. Must be set for enumeration members.
     */
    public HDF5CompoundMemberMapping enumType(HDF5EnumerationType enumType)
    {
        this.enumTypeOrNull = enumType;
        checkEnumArrayRank();
        this.memberClassOrNull =
                (memberTypeLength == 0) ? HDF5EnumerationValue.class
                        : HDF5EnumerationValueArray.class;
        return this;
    }

    HDF5EnumerationType tryGetEnumerationType()
    {
        return enumTypeOrNull;
    }

    /**
     * Sets the data type variant to use for the mapping.
     */
    public HDF5CompoundMemberMapping typeVariant(HDF5DataTypeVariant typeVariant)
    {
        this.typeVariantOrNull = typeVariant;
        return this;
    }

    HDF5DataTypeVariant tryGetTypeVariant()
    {
        return typeVariantOrNull;
    }
}
