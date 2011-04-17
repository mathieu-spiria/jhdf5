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

import static ch.systemsx.cisd.hdf5.HDF5BaseReader.REFERENCE_SIZE_IN_BYTES;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_REF_OBJ;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5ReferenceWriter}.
 * 
 * @author Bernd Rinn
 */
public class HDF5ReferenceWriter implements IHDF5ReferenceWriter
{

    private final HDF5BaseWriter baseWriter;

    HDF5ReferenceWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setObjectReferenceAttribute(String objectPath, String name,
            String referencedObjectPath)
    {
        assert objectPath != null;
        assert name != null;
        assert referencedObjectPath != null;

        baseWriter.checkOpen();
        final byte[] reference =
                baseWriter.h5.createObjectReference(baseWriter.fileId, referencedObjectPath);
        baseWriter.setAttribute(objectPath, name, H5T_STD_REF_OBJ, H5T_STD_REF_OBJ, reference);
    }

    public void setObjectReferenceArrayAttribute(final String objectPath, final String name,
            final String[] value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> setAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int typeId =
                            baseWriter.h5.createArrayType(H5T_STD_REF_OBJ, value.length, registry);
                    final byte[] references = new byte[REFERENCE_SIZE_IN_BYTES * value.length];
                    int ofs = 0;
                    for (String referencedObjectPath : value)
                    {
                        final byte[] reference =
                                baseWriter.h5.createObjectReference(baseWriter.fileId,
                                        referencedObjectPath);
                        System.arraycopy(reference, 0, references, ofs, REFERENCE_SIZE_IN_BYTES);
                        ofs += REFERENCE_SIZE_IN_BYTES;
                    }
                    baseWriter.setAttribute(objectPath, name, typeId, typeId, references);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeObjectReference(String objectPath, String referencedObjectPath)
    {
        assert objectPath != null;
        assert referencedObjectPath != null;

        baseWriter.checkOpen();
        final byte[] reference =
                baseWriter.h5.createObjectReference(baseWriter.fileId, referencedObjectPath);
        baseWriter.writeScalar(objectPath, H5T_STD_REF_OBJ, H5T_STD_REF_OBJ, reference);
    }

    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath)
    {
        writeObjectReferenceArray(objectPath, referencedObjectPath,
                HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath, final HDF5IntStorageFeatures features)
    {
        assert referencedObjectPath != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final byte[] references =
                            new byte[referencedObjectPath.length * REFERENCE_SIZE_IN_BYTES];
                    for (int i = 0; i < referencedObjectPath.length; ++i)
                    {
                        final byte[] reference =
                                baseWriter.h5.createObjectReference(baseWriter.fileId,
                                        referencedObjectPath[i]);
                        System.arraycopy(reference, 0, references, i * REFERENCE_SIZE_IN_BYTES,
                                REFERENCE_SIZE_IN_BYTES);
                    }
                    final int dataSetId =
                            baseWriter.getOrCreateDataSetId(objectPath, H5T_STD_REF_OBJ, new long[]
                                { referencedObjectPath.length }, REFERENCE_SIZE_IN_BYTES, features,
                                    registry);
                    H5Dwrite(dataSetId, H5T_STD_REF_OBJ, H5S_ALL, H5S_ALL, H5P_DEFAULT, references);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }
}
