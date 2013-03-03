/*
 * Copyright 2012 ETH Zuerich, CISD
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
 * An interface with legacy methods for writing enumeration values from HDF5 files. Do not use in any
 * new code as it will be removed in a future version of JHDF5.
 * 
 * @author Bernd Rinn
 */
@Deprecated
public interface IHDF5EnumBasicWriter
{

    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType getEnumType(String dataTypeName, String[] values)
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
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType getEnumType(String dataTypeName, String[] values,
            boolean check) throws HDF5JavaException;

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Sets an enum attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void setEnumAttribute(String objectPath, String name,
            HDF5EnumerationValue value);

    /**
     * Sets an enum attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void setEnumAttribute(String objectPath, String name, Enum<?> value);

    /**
     * Sets an enum array attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void setEnumArrayAttribute(String objectPath, String name,
            HDF5EnumerationValueArray value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out an enum value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void writeEnum(String objectPath, HDF5EnumerationValue value);

    /**
     * Writes out an enum value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value of the data set.
     */
    public <T extends Enum<T>> void writeEnum(String objectPath, Enum<T> value);

    /**
     * Writes out an enum value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param options The allowed values of the enumeration type.
     * @param value The value of the data set.
     */
    public void writeEnum(String objectPath, String[] options, String value);

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data);

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     */
    public <T extends Enum<T>> void writeEnumArray(String objectPath, Enum<T>[] data);

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param options The allowed values of the enumeration type.
     * @param data The data to write.
     */
    public void writeEnumArray(String objectPath, String[] options, String[] data);

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param features The storage features of the data set. Note that for scaling compression the
     *            compression factor is ignored. Instead, the scaling factor is computed from the
     *            number of entries in the enumeration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data,
            HDF5IntStorageFeatures features);

    /**
     * Creates am enum array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType createEnumArray(String objectPath,
            HDF5EnumerationType enumType, int size);

    /**
     * Creates am enum array (of rank 1). The initial size of the array is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param size The size of the enum array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType createEnumArray(String objectPath,
            HDF5EnumerationType enumType, long size, int blockSize);

    /**
     * Creates am enum array (of rank 1). The initial size of the array is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param size The size of the enum array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType createEnumArray(String objectPath,
            HDF5EnumerationType enumType, long size, int blockSize,
            HDF5IntStorageFeatures features);

    /**
     * Creates am enum array (of rank 1). The initial size of the array is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @param size The size of the enum array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public HDF5EnumerationType createEnumArray(String objectPath,
            HDF5EnumerationType enumType, long size,
            HDF5IntStorageFeatures features);

    /**
     * Writes out a block of an enum array (of rank 1). The data set needs to have been created by
     * {@link #createEnumArray(String, HDF5EnumerationType, long, int, HDF5IntStorageFeatures)}
     * beforehand. Obviously the {@link HDF5EnumerationType} of the create call and this call needs
     * to match.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createEnumArray(String, HDF5EnumerationType, long, int, HDF5IntStorageFeatures)} call
     * that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The value of {@link HDF5EnumerationValueArray#getLength()}
     *            defines the block size. Must not be <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void writeEnumArrayBlock(String objectPath, HDF5EnumerationValueArray data,
            long blockNumber);

    /**
     * Writes out a block of an enum array (of rank 1). The data set needs to have been created by
     * {@link #createEnumArray(String, HDF5EnumerationType, long, int, HDF5IntStorageFeatures)}
     * beforehand. Obviously the {@link HDF5EnumerationType} of the create call and this call needs
     * to match.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createEnumArray(String, HDF5EnumerationType, long, int, HDF5IntStorageFeatures)} call
     * that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The value of {@link HDF5EnumerationValueArray#getLength()}
     *            defines the block size. Must not be <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be
     *            <code><= data.getLength()</code> )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public void writeEnumArrayBlockWithOffset(String objectPath,
            HDF5EnumerationValueArray data, int dataSize, long offset);

}
