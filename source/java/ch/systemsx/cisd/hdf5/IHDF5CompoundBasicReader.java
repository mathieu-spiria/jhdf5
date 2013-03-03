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

import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.IHDF5CompoundInformationRetriever.IByteArrayInspector;

/**
 * An interface with legacy methods for reading compound values from HDF5 files. Do not use in any
 * new code as it will be removed in a future version of JHDF5.
 * 
 * @author Bernd Rinn
 */
@Deprecated
public interface IHDF5CompoundBasicReader
{
    // /////////////////////
    // Information
    // /////////////////////

    /**
     * Returns the member information for the committed compound data type <var>compoundClass</var>
     * (using its "simple name") in the order that the members appear in the compound type. It is a
     * failure condition if this compound data type does not exist.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass);

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>
     * in the order that the members appear in the compound type. It is a failure condition if this
     * compound data type does not exist. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the compound data type to get the member information for.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(final String dataTypeName);

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var> in the order
     * that the members appear in the compound type. It is a failure condition if this data set does
     * not exist or is not of compound type.
     * <p>
     * Call <code>Arrays.sort(compoundInformation)</code> to sort the array in alphabetical order of
     * names.
     * 
     * @throws HDF5JavaException If the data set is not of compound type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var> in the order
     * that the members appear in the compound type. The returned array will contain the members in
     * alphabetical order, if <var>sortAlphabetically</var> is <code>true</code> or else in the
     * order of definition of the compound type. It is a failure condition if this data set does not
     * exist or is not of compound type.
     * <p>
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     * @deprecated Use {@link #getCompoundDataSetInformation(String)} and
     *             <code>Arrays.sort(compoundInformation)</code>, if needed.
     */
    @Deprecated
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath,
            final boolean sortAlphabetically) throws HDF5JavaException;

    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type for this HDF5 file, using the default name chosen by JHDF5 which is
     * based on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name,
            final Class<T> pojoClass);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getInferredCompoundType(final Class<T> pojoClass);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound to infer the HDF5 compound type from.
     * @param hints The hints to provide to the mapping procedure.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final T template,
            HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final T template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>T</var>.
     * 
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getInferredCompoundType(final T template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length as <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundType<List<?>> getInferredCompoundType(final String name,
            List<String> memberNames, List<?> template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length as <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundType<List<?>> getInferredCompoundType(List<String> memberNames,
            List<?> template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length than <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundType<Object[]> getInferredCompoundType(final String name,
            String[] memberNames, Object[] template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length than <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public HDF5CompoundType<Object[]> getInferredCompoundType(String[] memberNames,
            Object[] template);

    /**
     * Returns the compound type for the given compound data set in <var>objectPath</var>, mapping
     * it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset to get the type from.
     * @param pojoClass The class to use for the mapping.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath, Class<T> pojoClass);

    /**
     * Returns the compound type for the given compound attribute in <var>attributeName</var> of
     * <var>objectPath</var>, mapping it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset to get the type from.
     * @param pojoClass The class to use for the mapping.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getAttributeCompoundType(String objectPath,
            String attributeName, Class<T> pojoClass);

    /**
     * Returns the named compound type with name <var>dataTypeName</var> from file, mapping it to
     * <var>pojoClass</var>. If the <var>dataTypeName</var> starts with '/', it will be considered a
     * data type path instead of a data type name.
     * <p>
     * <em>Note:</em> This method only works for compound data types 'committed' to the HDF5 file.
     * For files written with JHDF5 this will always be true, however, files created with other
     * libraries may not choose to commit compound data types.
     * 
     * @param dataTypeName The path to a committed data type, if starting with '/', or a name of a
     *            committed data type otherwise.
     * @param pojoClass The class to use for the mapping.
     * @return The compound data type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> pojoClass);

    /**
     * Returns the named compound type with name <var>dataTypeName</var> from file, mapping it to
     * <var>pojoClass</var>. This method will use the default name for the compound data type as
     * chosen by JHDF5 and thus will likely only work on files written with JHDF5. The default name
     * is based on the simple name of <var>compoundType</var>.
     * 
     * @param pojoClass The class to use for the mapping and to get the name of named data type
     *            from.
     * @return The compound data type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> pojoClass);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T readCompound(String objectPath, HDF5CompoundType<T> type) throws HDF5JavaException;

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param pojoClass The class to return the result in. Use {@link HDF5CompoundDataMap} to get it
     *            in a map, {@link HDF5CompoundDataList} to get it in a list, and
     *            <code>Object[]</code> to get it in an array, or use a pojo (Data Transfer Object),
     *            in which case the compound members will be mapped to Java fields.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set or if the
     *             mapping between the compound type and the POJO is not complete.
     * @see CompoundType
     * @see CompoundElement
     */
    public <T> T readCompound(String objectPath, Class<T> pojoClass) throws HDF5JavaException;

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into a Java object.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T readCompound(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArray(String objectPath, HDF5CompoundType<T> type)
            throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArray(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param pojoClass The class to return the result in. Use {@link HDF5CompoundDataMap} to get it
     *            in a map, {@link HDF5CompoundDataList} to get it in a list, and
     *            <code>Object[]</code> to get it in an array, or use a pojo (Data Transfer Object),
     *            in which case the compound members will be mapped to Java fields.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set or if the
     *             mapping between the compound type and the POJO is not complete.
     * @see CompoundType
     * @see CompoundElement
     */
    public <T> T[] readCompoundArray(String objectPath, Class<T> pojoClass)
            throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long blockNumber) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long blockNumber, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param offset The offset of the block to read (starting with 0).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long offset) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param offset The offset of the block to read (starting with 0).
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> T[] readCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long offset, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1 or not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            HDF5CompoundType<T> type) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1 or not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            HDF5CompoundType<T> type, IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param pojoClass The class to return the result in. Use {@link HDF5CompoundDataMap} to get it
     *            in a map, {@link HDF5CompoundDataList} to get it in a list, and
     *            <code>Object[]</code> to get it in an array, or use a pojo (Data Transfer Object),
     *            in which case the compound members will be mapped to Java fields.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1 or not a compound data set.
     * @throws HDF5JavaException If the data set is not of rank 1, not a compound data set or if the
     *             mapping between the compound type and the POJO is not complete.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            Class<T> pojoClass) throws HDF5JavaException;

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArray(String objectPath, HDF5CompoundType<T> type)
            throws HDF5JavaException;

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param pojoClass The class to return the result in. Use {@link HDF5CompoundDataMap} to get it
     *            in a map, {@link HDF5CompoundDataList} to get it in a list, and
     *            <code>Object[]</code> to get it in an array, or use a pojo (Data Transfer Object),
     *            in which case the compound members will be mapped to Java fields.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set or if the
     *             mapping between the compound type and the POJO is not complete.
     * @see CompoundType
     * @see CompoundElement
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArray(String objectPath, Class<T> pojoClass)
            throws HDF5JavaException;

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param blockNumber The number of the block to write along each axis.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int[] blockDimensions, long[] blockNumber) throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param blockNumber The number of the block to write along each axis.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int[] blockDimensions, long[] blockNumber, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param offset The offset of the block to write in the data set along each axis.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, int[] blockDimensions, long[] offset)
            throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param offset The offset of the block to write in the data set along each axis.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, int[] blockDimensions, long[] offset,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, HDF5CompoundType<T> type) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, HDF5CompoundType<T> type, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param pojoClass The class to return the result in. Use {@link HDF5CompoundDataMap} to get it
     *            in a map, {@link HDF5CompoundDataList} to get it in a list, and
     *            <code>Object[]</code> to get it in an array.
     * @see HDF5DataBlock
     * @see CompoundType
     * @see CompoundElement
     * @throws HDF5JavaException If the data set is not a compound data set or if the mapping
     *             between the compound type and the POJO is not complete.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#compound()} instead.
     */
    @Deprecated
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, Class<T> pojoClass) throws HDF5JavaException;

}
