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

import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation.DataTypeInfoOptions;

/**
 * An interface to get information about HDF5 compound data sets and types and map to map compound
 * types to plain old Java classes.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5CompoundInformationRetriever
{

    /**
     * An interface for inspecting the byte array of compounds and compound arrays just after they
     * are read from or before they are written to the HDF5 file.
     */
    public interface IByteArrayInspector
    {
        /**
         * Called with the byte array. The method can change the <var>byteArray</var> but does so on
         * its own risk!
         */
        void inspect(byte[] byteArray);
    }

    // /////////////////////
    // Information
    // /////////////////////

    /**
     * Returns the member information for the committed compound data type <var>compoundClass</var>
     * (using its "simple name") in the order that the members appear in the compound type. It is a
     * failure condition if this compound data type does not exist.
     */
    public <T> HDF5CompoundMemberInformation[] getMemberInfo(Class<T> compoundClass);

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>
     * in the order that the members appear in the compound type. It is a failure condition if this
     * compound data type does not exist. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the compound data type to get the member information for.
     */
    public HDF5CompoundMemberInformation[] getMemberInfo(String dataTypeName);

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>
     * in the order that the members appear in the compound type. It is a failure condition if this
     * compound data type does not exist. If the <var>dataTypeName</var> starts with '/', it will be
     * considered a data type path instead of a data type name.
     * 
     * @param dataTypeName The name of the compound data type to get the member information for.
     * @param dataTypeInfoOptions The options on which information to get about the member data
     *            types.
     */
    public HDF5CompoundMemberInformation[] getMemberInfo(String dataTypeName,
            DataTypeInfoOptions dataTypeInfoOptions);

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var> in the order
     * that the members appear in the compound type. It is a failure condition if this data set does
     * not exist or is not of compound type.
     * <p>
     * Call <code>Arrays.sort(compoundInformation)</code> to sort the array in alphabetical order of
     * names.
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     */
    public HDF5CompoundMemberInformation[] getDataSetInfo(String dataSetPath)
            throws HDF5JavaException;

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var> in the order
     * that the members appear in the compound type. It is a failure condition if this data set does
     * not exist or is not of compound type.
     * <p>
     * Call <code>Arrays.sort(compoundInformation)</code> to sort the array in alphabetical order of
     * names.
     * 
     * @param dataSetPath The name of the data set to get the member information for.
     * @param dataTypeInfoOptions The options on which information to get about the member data
     *            types.
     * @throws HDF5JavaException If the data set is not of type compound.
     */
    public HDF5CompoundMemberInformation[] getDataSetInfo(String dataSetPath,
            DataTypeInfoOptions dataTypeInfoOptions) throws HDF5JavaException;

    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getType(String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type for this HDF5 file, using the default name chosen by JHDF5 which is
     * based on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getType(Class<T> pojoClass, HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param hints The hints to provide to the mapping procedure.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(String name, Class<T> pojoClass,
            HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(String name, Class<T> pojoClass);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     */
    public <T> HDF5CompoundType<T> getInferredType(Class<T> pojoClass);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound to infer the HDF5 compound type from.
     * @param hints The hints to provide to the mapping procedure.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(String name, T template,
            HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(String name, T template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>T</var>.
     * 
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(T template);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound array to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(final String name, final T[] template);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param template The compound array to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(final T[] template);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param template The compound array to infer the HDF5 compound type from.
     * @param hints The hints to provide to the mapping procedure.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredType(String name, T[] template,
            HDF5CompoundMappingHints hints);
    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length as <var>memberNames</var>.
     * @param hints The hints to provide to the mapping procedure.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public HDF5CompoundType<List<?>> getInferredType(String name, List<String> memberNames,
            List<?> template, HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length as <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public HDF5CompoundType<List<?>> getInferredType(String name, List<String> memberNames,
            List<?> template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length as <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public HDF5CompoundType<List<?>> getInferredType(List<String> memberNames, List<?> template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param name The name of the compound type in the HDF5 file.
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length than <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public HDF5CompoundType<Object[]> getInferredType(String name, String[] memberNames,
            Object[] template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java types of
     * the members.
     * 
     * @param memberNames The names of the members.
     * @param template The compound to infer the HDF5 compound type from. Needs to have the same
     *            length than <var>memberNames</var>.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public HDF5CompoundType<Object[]> getInferredType(String[] memberNames,
            Object[] template);

    /**
     * Returns the compound type for the given compound data set in <var>objectPath</var>, mapping
     * it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset to get the type from.
     * @param pojoClass The class to use for the mapping.
     * @param members The mapping from the Java compound type to the HDF5 type.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getDataSetType(String objectPath, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);
    
    /**
     * Returns the compound type for the given compound data set in <var>objectPath</var>, mapping
     * it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset to get the type from.
     * @param pojoClass The class to use for the mapping.
     * @param hints The hints to provide to the mapping procedure.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getDataSetType(String objectPath, Class<T> pojoClass,
            HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type for the given compound data set in <var>objectPath</var>, mapping
     * it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset to get the type from.
     * @param pojoClass The class to use for the mapping.
     */
    public <T> HDF5CompoundType<T> getDataSetType(String objectPath, Class<T> pojoClass);

    /**
     * Returns the compound type for the given compound attribute in <var>attributeName</var> of
     * <var>objectPath</var>, mapping it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset.
     * @param attributeName The name of the attribute to get the type for.
     * @param pojoClass The class to use for the mapping.
     */
    public <T> HDF5CompoundType<T> getAttributeType(String objectPath,
            String attributeName, Class<T> pojoClass);

    /**
     * Returns the compound type for the given compound attribute in <var>attributeName</var> of
     * <var>objectPath</var>, mapping it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset.
     * @param attributeName The name of the attribute to get the type for.
     * @param pojoClass The class to use for the mapping.
     * @param hints The hints to provide to the mapping procedure.
     */
    public <T> HDF5CompoundType<T> getAttributeType(String objectPath,
            String attributeName, Class<T> pojoClass, HDF5CompoundMappingHints hints);

    /**
     * Returns the compound type for the given compound attribute in <var>attributeName</var> of
     * <var>objectPath</var>, mapping it to <var>pojoClass</var>.
     * 
     * @param objectPath The path of the compound dataset.
     * @param attributeName The name of the attribute to get the type for.
     * @param pojoClass The class to use for the mapping.
     * @param hints The hints to provide to the mapping procedure.
     * @param dataTypeInfoOptions The options on which information to get about the member data
     *            types.
     */
    public <T> HDF5CompoundType<T> getAttributeType(String objectPath,
            String attributeName, Class<T> pojoClass, HDF5CompoundMappingHints hints,
            DataTypeInfoOptions dataTypeInfoOptions);

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
     */
    public <T> HDF5CompoundType<T> getNamedType(String dataTypeName, Class<T> pojoClass);

    /**
     * Returns the named compound type with name <var>dataTypeName</var> from file, mapping it to
     * <var>pojoClass</var>. This method will use the default name for the compound data type as
     * chosen by JHDF5 and thus will likely only work on files written with JHDF5. The default name
     * is based on the simple name of <var>compoundType</var>.
     * 
     * @param pojoClass The class to use for the mapping and to get the name of named data type
     *            from.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getNamedType(Class<T> pojoClass);

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
     * @param hints The hints to provide to the mapping procedure.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getNamedType(String dataTypeName, Class<T> pojoClass,
            HDF5CompoundMappingHints hints);

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
     * @param dataTypeInfoOptions The options on which information to get about the member data
     *            types.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getNamedType(String dataTypeName, Class<T> pojoClass,
            DataTypeInfoOptions dataTypeInfoOptions);

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
     * @param hints The hints to provide to the mapping procedure.
     * @param dataTypeInfoOptions The options on which information to get about the member data
     *            types.
     * @return The compound data type.
     */
    public <T> HDF5CompoundType<T> getNamedType(String dataTypeName, Class<T> pojoClass,
            HDF5CompoundMappingHints hints, DataTypeInfoOptions dataTypeInfoOptions);
}
