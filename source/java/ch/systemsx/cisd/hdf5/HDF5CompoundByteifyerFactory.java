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

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ch.systemsx.cisd.hdf5.HDF5ValueObjectByteifyer.FileInfoProvider;

/**
 * A factory for {@link HDF5MemberByteifyer}s.
 * 
 * @author Bernd Rinn
 */
class HDF5CompoundByteifyerFactory
{

    private static List<IHDF5CompoundMemberBytifyerFactory> memberFactories =
            new LinkedList<IHDF5CompoundMemberBytifyerFactory>();

    static
    {
        memberFactories.add(new HDF5CompoundMemberByteifyerBooleanFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerIntFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerLongFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerShortFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerByteFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerFloatFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerDoubleFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerStringFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerBitSetFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerDateFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerEnumFactory());
        memberFactories.add(new HDF5CompoundMemberByteifyerEnumArrayFactory());
    }

    /**
     * The type of access to the information.
     */
    enum AccessType
    {
        FIELD, MAP, LIST, ARRAY
    }

    /**
     * The interface for member factories.
     */
    interface IHDF5CompoundMemberBytifyerFactory
    {
        /**
         * Returns <code>true</code> if this factory can handle a member of type <code>clazz</code>.
         */
        boolean canHandle(Class<?> clazz);

        /**
         * Creates a byteifyer.
         */
        HDF5MemberByteifyer createBytifyer(final AccessType accessType, final Field fieldOrNull,
                final HDF5CompoundMemberMapping member, Class<?> memberClazz, final int index,
                final int offset, final FileInfoProvider fileInfoProvider);
    }

    static HDF5MemberByteifyer[] createMemberByteifyers(Class<?> clazz,
            FileInfoProvider fileInfoProvider, HDF5CompoundMemberMapping[] members)
    {
        final HDF5MemberByteifyer[] result = new HDF5MemberByteifyer[members.length];
        int offset = 0;
        for (int i = 0; i < result.length; ++i)
        {
            final AccessType accessType = getAccessType(clazz);
            final Field fieldOrNull =
                    (accessType == AccessType.FIELD) ? members[i].getField(clazz) : null;
            final Class<?> memberClazzOrNull =
                    (fieldOrNull != null) ? fieldOrNull.getType() : members[i].tryGetMemberClass();
            final IHDF5CompoundMemberBytifyerFactory factory =
                    findFactory(memberClazzOrNull, members[i].getMemberName());
            result[i] =
                    factory.createBytifyer(accessType, fieldOrNull, members[i], memberClazzOrNull,
                            i, offset, fileInfoProvider);
            offset += result[i].getSizeInBytes();
        }
        return result;
    }

    //
    // Auxiliary getter and setter methods.
    //

    private static IHDF5CompoundMemberBytifyerFactory findFactory(Class<?> memberClazz,
            String memberName)
    {
        if (memberClazz == null)
        {
            throw new IllegalArgumentException("No type given for member '" + memberName + "'.");
        }
        for (IHDF5CompoundMemberBytifyerFactory factory : memberFactories)
        {
            if (factory.canHandle(memberClazz))
            {
                return factory;
            }
        }
        throw new IllegalArgumentException("The member '" + memberName + "' is of type '"
                + memberClazz.getCanonicalName()
                + "' which cannot be handled by an HDFMemberByteifyer.");
    }

    private static AccessType getAccessType(Class<?> clazz)
    {
        if (Map.class.isAssignableFrom(clazz))
        {
            return AccessType.MAP;
        } else if (List.class.isAssignableFrom(clazz))
        {
            return AccessType.LIST;
        } else if (Object[].class == clazz)
        {
            return AccessType.ARRAY;
        } else
        {
            return AccessType.FIELD;
        }
    }

    @SuppressWarnings("unchecked")
    static Object getMap(Object obj, final String name)
    {
        return ((Map<String, Object>) obj).get(name);
    }

    @SuppressWarnings("unchecked")
    static Object getList(Object obj, final int index)
    {
        return ((List<Object>) obj).get(index);
    }

    static Object getArray(Object obj, final int index)
    {
        return ((Object[]) obj)[index];
    }

    @SuppressWarnings("unchecked")
    static void putMap(final Object obj, final String memberName, final Object value)
    {
        ((Map<String, Object>) obj).put(memberName, value);
    }

    @SuppressWarnings("unchecked")
    static void setList(final Object obj, final int index, final Object value)
    {
        ((List<Object>) obj).set(index, value);
    }

    static void setArray(final Object obj, final int index, final Object value)
    {
        ((Object[]) obj)[index] = value;
    }

}
