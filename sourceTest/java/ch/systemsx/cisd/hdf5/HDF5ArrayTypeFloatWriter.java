/*
 * Copyright 2009 ETH Zuerich, CISD
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

import static ncsa.hdf.hdf5lib.H5.H5Aclose;
import static ncsa.hdf.hdf5lib.H5.H5Acreate;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_float;
import static ncsa.hdf.hdf5lib.H5.H5Sclose;
import static ncsa.hdf.hdf5lib.H5.H5Screate_simple;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_FLOAT;
import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A writer for array type data sets.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArrayTypeFloatWriter
{

    private final HDF5BaseWriter baseWriter;

    HDF5ArrayTypeFloatWriter(HDF5Writer writer)
    {
        baseWriter = writer.getBaseWriter();
    }

    public void writeFloatArrayArrayType(final String objectPath, final float[] data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_FLOAT, data.length, registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_IEEE_F32LE, data.length, registry);
                    final int dataSetId =
                            baseWriter.h5.createScalarDataSet(baseWriter.fileId, storageTypeId,
                                    objectPath, registry);
                    H5Dwrite_float(dataSetId, memoryTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeFloatArrayArrayType(final String objectPath, final MDFloatArray data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_FLOAT, data.dimensions(),
                                    registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_IEEE_F32LE, data.dimensions(),
                                    registry);
                    final int dataSetId =
                            baseWriter.h5.createScalarDataSet(baseWriter.fileId, storageTypeId,
                                    objectPath, registry);
                    H5Dwrite_float(dataSetId, memoryTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data
                            .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void setFloatArrayAttributeDimensional(final String objectPath, final String name,
            final float[] value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> addAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = new long[]
                        { value.length };
                    final int dataSpaceId =
                            H5Screate_simple(dimensions.length, dimensions, dimensions);
                    registry.registerCleanUp(new Runnable()
                        {
                            public void run()
                            {
                                H5Sclose(dataSpaceId);
                            }
                        });
                    final int objectId =
                            baseWriter.h5.openObject(baseWriter.fileId, objectPath, registry);
                    final int attributeId =
                            createAttribute(objectId, name, H5T_IEEE_F32LE, dataSpaceId, registry);
                    baseWriter.h5.writeAttribute(attributeId, H5T_NATIVE_FLOAT, HDFNativeData
                            .floatToByte(value));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private int createAttribute(int locationId, String attributeName, int dataTypeId,
            int dataSpaceId, ICleanUpRegistry registry)
    {
        final int attributeId =
                H5Acreate(locationId, attributeName, dataTypeId, dataSpaceId, H5P_DEFAULT,
                        H5P_DEFAULT);
        registry.registerCleanUp(new Runnable()
            {
                public void run()
                {
                    H5Aclose(attributeId);
                }
            });
        return attributeId;
    }

}
