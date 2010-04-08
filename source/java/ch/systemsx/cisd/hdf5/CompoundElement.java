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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * An annotation that describes the properties of a field as a member of an HDF5 compound data type.
 * 
 * @author Bernd Rinn
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface CompoundElement
{

    /**
     * The name of the member in the compound type. Leave empty to use the field name as member name.
     */
    String memberName() default "";

    /**
     * The length / dimensions of the compound member. Is required for compound members that have a
     * variable length, e.g. strings or primitive arrays. Ignored for compound members that have a
     * fixed length, e.g. a float field.
     */
    int[] dimensions() default 0;
}
