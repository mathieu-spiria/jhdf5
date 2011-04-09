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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_REFERENCE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_REF_OBJ;
import static ch.systemsx.cisd.hdf5.HDF5BaseReader.REFERENCE_SIZE_IN_BYTES;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A reader for HDF5 references.
 * 
 * @author Bernd Rinn
 */
public class HDF5ReferenceReader implements IHDF5ReferenceReader
{

    private final HDF5BaseReader baseReader;

    HDF5ReferenceReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public String getObjectReferenceAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final int objectReferenceDataTypeId =
                            baseReader.h5.getDataTypeForAttribute(attributeId, registry);
                    final boolean isReference =
                            (baseReader.h5.getClassType(objectReferenceDataTypeId) == H5T_REFERENCE);
                    if (isReference == false)
                    {
                        throw new IllegalArgumentException("Attribute " + attributeName
                                + " of object " + objectPath + " needs to be a Reference.");
                    }
                    final byte[] reference =
                            baseReader.h5.readAttributeAsByteArray(attributeId,
                                    objectReferenceDataTypeId, REFERENCE_SIZE_IN_BYTES);
                    return baseReader.h5.getReferencedObjectName(attributeId, reference);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    public String[] getObjectReferenceArrayAttribute(final String objectPath,
            final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> getAttributeRunnable =
                new ICallableWithCleanUp<String[]>()
                    {
                        public String[] call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final int attributeTypeId =
                                    baseReader.h5.getDataTypeForAttribute(attributeId, registry);
                            final int memoryTypeId;
                            final int len;
                            if (baseReader.h5.getClassType(attributeTypeId) == H5T_ARRAY)
                            {
                                final int[] arrayDimensions =
                                        baseReader.h5.getArrayDimensions(attributeTypeId);
                                if (arrayDimensions.length != 1)
                                {
                                    throw new HDF5JavaException(
                                            "Array needs to be of rank 1, but is of rank "
                                                    + arrayDimensions.length);
                                }
                                len = arrayDimensions[0];
                                memoryTypeId =
                                        baseReader.h5.createArrayType(H5T_STD_REF_OBJ, len,
                                                registry);
                            } else
                            {
                                final long[] arrayDimensions =
                                        baseReader.h5.getDataDimensionsForAttribute(attributeId,
                                                registry);
                                memoryTypeId = H5T_STD_REF_OBJ;
                                len = HDF5Utils.getOneDimensionalArraySize(arrayDimensions);
                            }
                            final byte[] references =
                                    baseReader.h5.readAttributeAsByteArray(attributeId,
                                            memoryTypeId, REFERENCE_SIZE_IN_BYTES * len);
                            final String[] referencedObjectPaths = new String[len];
                            for (int i = 0; i < len; ++i)
                            {
                                referencedObjectPaths[i] =
                                        baseReader.h5.getReferencedObjectName(attributeId,
                                                references, i * REFERENCE_SIZE_IN_BYTES);
                            }
                            return referencedObjectPaths;
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public String readObjectReference(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int objectReferenceDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    final boolean isReference =
                            (baseReader.h5.getClassType(objectReferenceDataTypeId) == H5T_REFERENCE);
                    if (isReference == false)
                    {
                        throw new IllegalArgumentException("Dataset " + objectPath
                                + " needs to be a Reference.");
                    }
                    final byte[] reference = new byte[REFERENCE_SIZE_IN_BYTES];
                    baseReader.h5.readDataSet(dataSetId, objectReferenceDataTypeId, reference);
                    return baseReader.h5.getReferencedObjectName(dataSetId, reference);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

}
