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

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * An interface for retrieving HDF5 enum types. Depending on whether it is reader or a writer that
 * implements it, non-existing enum types may be created by calling the methods of this interface or
 * an exception may be thrown.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5EnumTypeRetriever
{
    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. If the type is read from the
     * file, it will check the type in the file with the <var>values</var>. If the
     * <var>dataTypeName</var> starts with '/', it will be considered a data type path instead of a
     * data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    public HDF5EnumerationType getEnumType(final String dataTypeName, final String[] values)
            throws HDF5JavaException;

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. If the
     * <var>dataTypeName</var> starts with '/', it will be considered a data type path instead of a
     * data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @param check If <code>true</code> and if the data type already exists, check whether it is
     *            compatible with the <var>values</var> provided.
     * @throws HDF5JavaException If <code>check = true</code>, the data type exists and is not
     *             compatible with the <var>values</var> provided.
     */
    public HDF5EnumerationType getEnumType(final String dataTypeName, final String[] values,
            final boolean check) throws HDF5JavaException;

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     * @param enumClass The enumeration class to get the values from.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>enumClass</var> provided.
     */
    public HDF5EnumerationType getEnumType(final String dataTypeName,
            final Class<? extends Enum<?>> enumClass) throws HDF5JavaException;

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     * @param enumClass The enumeration class to get the values from.
     * @param check If <code>true</code> and if the data type already exists, check whether it is
     *            compatible with the <var>enumClass</var> provided.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    public <T extends Enum<?>> HDF5EnumerationType getEnumType(final String dataTypeName,
            final Class<T> enumClass, final boolean check) throws HDF5JavaException;

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. Will use the simple class name of <var>enumClass</var> as
     * the data type name.
     * 
     * @param enumClass The enumeration class to get the values from.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    public <T extends Enum<?>> HDF5EnumerationType getEnumType(final Class<T> enumClass)
            throws HDF5JavaException;

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. Will use the simple class name of <var>enumClass</var> as
     * the data type name.
     * 
     * @param enumClass The enumeration class to get the values from.
     * @param check If <code>true</code> and if the data type already exists, check whether it is
     *            compatible with the <var>enumClass</var> provided.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    public HDF5EnumerationType getEnumType(final Class<? extends Enum<?>> enumClass,
            final boolean check) throws HDF5JavaException;
}
