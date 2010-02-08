/*
 * Copyright 2010 ETH Zuerich, CISD
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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for reflection.
 * 
 * @author Bernd Rinn
 */
final class ReflectionUtils
{

    private ReflectionUtils()
    {
        // Cannot be instantiated
    }
    
    /**
     * Returns a map from field names to fields for all fields in the given <var>clazz</var>.
     */
    public static Map<String, Field> getFieldMap(final Class<?> clazz)
    {
        final Map<String, Field> fields = new HashMap<String, Field>();
        addFieldsToMap(clazz, fields);
        return fields;
    }

    private static void addFieldsToMap(final Class<?> clazz, final Map<String, Field> fields)
    {
        for (final Field field : clazz.getDeclaredFields())
        {
            fields.put(field.getName(), field);
        }
        final Class<?> superClass = clazz.getSuperclass();
        if (superClass != null)
        {
            addFieldsToMap(superClass, fields);
        }
    }

    /**
     * Ensures that the given <var>member</var> is accessible even if by definition it is not.
     */
    public static void ensureAccessible(final AccessibleObject member)
    {
        if (member.isAccessible() == false)
        {
            member.setAccessible(true);
        }
    }

    /**
     * Creates an object of <var>clazz</var> using the default constructor, making the default
     * constructor accessible if necessary.
     */
    public static <T> T newInstance(final Class<T> clazz) throws SecurityException,
            NoSuchMethodException, IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException
    {
        final Constructor<T> defaultConstructor = clazz.getDeclaredConstructor();
        ensureAccessible(defaultConstructor);
        return defaultConstructor.newInstance();

    }

}
