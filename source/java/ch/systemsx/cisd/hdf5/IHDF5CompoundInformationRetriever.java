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
         * its own risk!.
         */
        void inspect(byte[] byteArray);
    }

    // /////////////////////
    // Information
    // /////////////////////

    /**
     * Returns the member information for the committed compound data type <var>compoundClass</var>
     * (using its "simple name"). The returned array will contain the members in alphabetical order.
     * It is a failure condition if this compound data type does not exist.
     */
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass);

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>.
     * The returned array will contain the members in alphabetical order. It is a failure condition
     * if this compound data type does not exist. If the <var>dataTypeName</var> starts with '/', it
     * will be considered a data type path instead of a data type name.
     */
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(final String dataTypeName);

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var>. The returned
     * array will contain the members in alphabetical order. It is a failure condition if this data
     * set does not exist or is not of compound type.
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     */
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var>. The returned
     * array will contain the members in alphabetical order, if <var>sortAlphabetically</var> is
     * <code>true</code> or else in the order of definition of the compound type. It is a failure
     * condition if this data set does not exist or is not of compound type.
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     */
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
     */
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type for this HDF5 file, using the default name chosen by JHDF5 which is
     * based on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> pojoClass,
            HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name,
            final Class<T> pojoClass);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>pojoClass</var>.
     * 
     * @param pojoClass The plain old Java type that corresponds to this HDF5 type.
     */
    public <T> HDF5CompoundType<T> getInferredCompoundType(final Class<T> pojoClass);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, inferring the mapping from the
     * Java compound type to the HDF5 type by reflection.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, final T template);

    /**
     * Returns the compound type for this HDF5 file, inferring the mapping from the Java compound
     * type to the HDF5 type by reflection and using the default name chosen by JHDF5 which is based
     * on the simple name of <var>T</var>.
     * 
     * @param template The compound to infer the HDF5 compound type from.
     * @see HDF5CompoundMemberMapping#inferMapping
     */
    public <T> HDF5CompoundType<T> getInferredCompoundType(final T template);

    /**
     * Returns the compound type for the given compound data set in <var>objectPath</var>, mapping
     * it to <var>pojoClass</var>.
     */
    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath, Class<T> pojoClass);

    /**
     * Returns the named compound type with name <var>dataTypeName</var> from file, mapping it to
     * <var>pojoClass</var>. If the <var>dataTypeName</var> starts with '/', it will be considered a
     * data type path instead of a data type name.
     * <p>
     * <em>Note:</em> This method only works for compound data types 'committed' to the HDF5 file.
     * For files written with JHDF5 this will always be true, however, files created with other
     * libraries may not choose to commit compound data types.
     */
    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> pojoClass);

    /**
     * Returns the named compound type with name <var>dataTypeName</var> from file, mapping it to
     * <var>pojoClass</var>. This method will use the default name for the compound data type as
     * chosen by JHDF5 and thus will likely only work on files written with JHDF5. The default name
     * is based on the simple name of <var>compoundType</var>.
     */
    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> pojoClass);

}
