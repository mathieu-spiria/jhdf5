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

/**
 * The definition of a HDF5 compound type. For information on how to create and work with compound
 * types, have a look at {@link IHDF5CompoundInformationRetriever}. The simplest way of creating a
 * compound type for a Java class, is
 * {@link IHDF5CompoundInformationRetriever#getInferredCompoundType(Class)}.
 * <p>
 * Once you have a compound type, you may use methods like
 * {@link IHDF5Reader#readCompound(String, HDF5CompoundType)} and
 * {@link IHDF5Writer#writeCompound(String, HDF5CompoundType, Object)} and to read and write them.
 * 
 * @author Bernd Rinn
 */
public class HDF5CompoundType<T> extends HDF5DataType
{
    private final String nameOrNull;

    private final Class<T> compoundType;

    private final HDF5ValueObjectByteifyer<T> objectByteifyer;

    /**
     * Creates a new {@link HDF5CompoundType} for the given <var>compoundType</var> and the mapping
     * defined by <var>members</var>.
     * 
     * @param nameOrNull The name of this type, or <code>null</code>, if it is not known.
     * @param storageTypeId The storage data type id.
     * @param nativeTypeId The native (memory) data type id.
     * @param compoundType The Java type that corresponds to this type.
     * @param objectByteifer The byteifer to use to convert between the Java object and the HDF5
     *            file.
     */
    HDF5CompoundType(int fileId, int storageTypeId, int nativeTypeId, String nameOrNull,
            Class<T> compoundType, HDF5ValueObjectByteifyer<T> objectByteifer)
    {
        super(fileId, storageTypeId, nativeTypeId);
        assert compoundType != null;
        assert objectByteifer != null;

        this.nameOrNull = nameOrNull;
        this.compoundType = compoundType;
        this.objectByteifyer = objectByteifer;
    }

    /**
     * Returns the Java type of the compound.
     */
    public Class<T> getCompoundType()
    {
        return compoundType;
    }

    /**
     * Returns the byteifyer to convert between the Java type and the HDF5 type.
     */
    HDF5ValueObjectByteifyer<T> getObjectByteifyer()
    {
        return objectByteifyer;
    }

    @Override
    String getName()
    {
        return nameOrNull;
    }

}
