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

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * An interface that provides methods for reading enumeration values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5EnumReader
{
    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Use this method only when
     * you know that the type exists. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     */
    public HDF5EnumerationType getEnumType(final String dataTypeName);

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
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
     * Returns the enumeration type for the data set <var>dataSetPath</var>.
     * 
     * @param dataSetPath The name of data set to get the enumeration type for.
     */
    public HDF5EnumerationType getEnumTypeForObject(final String dataSetPath);

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads an <code>enum</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set as a String.
     * @throws HDF5JavaException If the attribute is not an enum type.
     */
    public String getEnumAttributeAsString(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    /**
     * Reads an <code>enum</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @throws HDF5JavaException If the attribute is not an enum type.
     */
    public HDF5EnumerationValue getEnumAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set as a String.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public String readEnumAsString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValue readEnum(final String objectPath) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * <p>
     * This method is faster than {@link #readEnum(String)} if the {@link HDF5EnumerationType} is
     * already available.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enum type in the HDF5 file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValue readEnum(final String objectPath, final HDF5EnumerationType enumType)
            throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath,
            final HDF5EnumerationType enumType) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set as an array of Strings.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public String[] readEnumArrayAsString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the return value of the
     *            {@link HDF5EnumerationValueArray#getLength()} returned if the data set is long
     *            enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public HDF5EnumerationValueArray readEnumArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber);

    /**
     * Reads an <code>Enum</code> value array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param blockSize The block size (this will be the return value of the
     *            {@link HDF5EnumerationValueArray#getLength()} returned if the data set is long
     *            enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public HDF5EnumerationValueArray readEnumArrayBlock(final String objectPath,
            final HDF5EnumerationType enumType, final int blockSize, final long blockNumber);

    /**
     * Reads an <code>Enum</code> value array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the return value of the
     *            {@link HDF5EnumerationValueArray#getLength()} returned if the data set is long
     *            enough).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset);

    /**
     * Reads an <code>Enum</code> value array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param blockSize The block size (this will be the return value of the
     *            {@link HDF5EnumerationValueArray#getLength()} returned if the data set is long
     *            enough).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(final String objectPath,
            final HDF5EnumerationType enumType, final int blockSize, final long offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            final String objectPath, final HDF5EnumerationType enumType) throws HDF5JavaException;


}
