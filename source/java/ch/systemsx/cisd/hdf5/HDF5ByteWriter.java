/*
 * Copyright 2009 ETH Zuerich, CISD.
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

import static ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures.INT_NO_COMPRESSION;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I8LE;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5ByteWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5ByteWriter implements IHDF5ByteWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5ByteWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setByteAttribute(final String objectPath, final String name, final byte value)
    {
        assert objectPath != null;
        assert name != null;

        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, H5T_STD_I8LE, H5T_NATIVE_INT8, HDFNativeData
                .byteToByte(value));
    }

    public void setByteArrayAttribute(final String objectPath, final String name, final byte[] value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> setAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT8, value.length, registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I8LE, value.length, registry);
                    baseWriter.setAttribute(objectPath, name, storageTypeId, memoryTypeId, value);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    public void setByteMDArrayAttribute(final String objectPath, final String name,
            final MDByteArray value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> addAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT8, value.dimensions(),
                                    registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I8LE, value.dimensions(),
                                    registry);
                    baseWriter.setAttribute(objectPath, name, storageTypeId, memoryTypeId, value
                            .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(addAttributeRunnable);
    }

    public void setByteMatrixAttribute(final String objectPath, final String name,
            final byte[][] value)
    {
        setByteMDArrayAttribute(objectPath, name, new MDByteArray(value));
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeByte(final String objectPath, final byte value)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, H5T_STD_I8LE, H5T_NATIVE_INT8, HDFNativeData
                .byteToByte(value));
    }

    public void writeByteArray(final String objectPath, final byte[] data)
    {
        writeByteArray(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeByteArray(final String objectPath, final byte[] data,
            final HDF5IntStorageFeatures features)
    {
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I8LE, new long[]
                                { data.length }, features, registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createByteArray(final String objectPath, final int blockSize)
    {
        createByteArray(objectPath, 0, blockSize, INT_NO_COMPRESSION);
    }

    public void createByteArray(final String objectPath, final long size, final int blockSize)
    {
        createByteArray(objectPath, size, blockSize, INT_NO_COMPRESSION);
    }

    public void createByteArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);

                    } else
                    {
                        baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void createByteArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeByteArrayBlock(final String objectPath, final byte[] data,
            final long blockNumber)
    {
        writeByteArrayBlockWithOffset(objectPath, data, data.length, data.length * blockNumber);
    }

    public void writeByteArrayBlockWithOffset(final String objectPath, final byte[] data,
            final int dataSize, final long offset)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] blockDimensions = new long[]
                        { dataSize };
                    final long[] slabStartOrNull = new long[]
                        { offset };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { offset + dataSize }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT8, memorySpaceId, dataSpaceId, H5P_DEFAULT,
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>byte</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeByteMatrix(final String objectPath, final byte[][] data)
    {
        writeByteMatrix(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeByteMatrix(final String objectPath, final byte[][] data,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;
        assert HDF5Utils.areMatrixDimensionsConsistent(data);

        writeByteMDArray(objectPath, new MDByteArray(data), features);
    }

    public void createByteMatrix(final String objectPath, final int blockSizeX, final int blockSizeY)
    {
        createByteMatrix(objectPath, 0, 0, blockSizeX, blockSizeY, INT_NO_COMPRESSION);
    }

    public void createByteMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY)
    {
        createByteMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, INT_NO_COMPRESSION);
    }

    public void createByteMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert sizeX >= 0;
        assert sizeY >= 0;
        assert blockSizeX >= 0 && (blockSizeX <= sizeX || sizeX == 0);
        assert blockSizeY >= 0 && (blockSizeY <= sizeY || sizeY == 0);

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = new long[]
                        { sizeX, sizeY };
                    final long[] blockDimensions = new long[]
                        { blockSizeX, blockSizeY };
                    baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, dimensions,
                            blockDimensions, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeByteMatrixBlock(final String objectPath, final byte[][] data,
            final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;
        assert data != null;

        writeByteMDArrayBlock(objectPath, new MDByteArray(data), new long[]
            { blockNumberX, blockNumberY });
    }

    public void writeByteMatrixBlockWithOffset(final String objectPath, final byte[][] data,
            final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeByteMDArrayBlockWithOffset(objectPath, new MDByteArray(data, new int[]
            { data.length, data[0].length }), new long[]
            { offsetX, offsetY });
    }

    public void writeByteMatrixBlockWithOffset(final String objectPath, final byte[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeByteMDArrayBlockWithOffset(objectPath, new MDByteArray(data, new int[]
            { dataSizeX, dataSizeY }), new long[]
            { offsetX, offsetY });
    }

    public void writeByteMDArray(final String objectPath, final MDByteArray data)
    {
        writeByteMDArray(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeByteMDArray(final String objectPath, final MDByteArray data,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I8LE,
                                    data.longDimensions(), features, registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, data
                            .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createByteMDArray(final String objectPath, final int[] blockDimensions)
    {
        createByteMDArray(objectPath, new long[blockDimensions.length], blockDimensions,
                INT_NO_COMPRESSION);
    }

    public void createByteMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions)
    {
        createByteMDArray(objectPath, dimensions, blockDimensions, INT_NO_COMPRESSION);
    }

    public void createByteMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert dimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    if (features.requiresChunking())
                    {
                        final long[] nullDimensions = new long[dimensions.length];
                        baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features,
                                nullDimensions, MDArray.toLong(dimensions), registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, MDArray
                                .toLong(dimensions), null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void createByteMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I8LE, features, dimensions,
                            MDArray.toLong(blockDimensions), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeByteMDArrayBlock(final String objectPath, final MDByteArray data,
            final long[] blockNumber)
    {
        assert blockNumber != null;

        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
        }
        writeByteMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray data,
            final long[] offset)
    {
        assert objectPath != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = data.longDimensions();
                    assert dimensions.length == offset.length;
                    final long[] dataSetDimensions = new long[dimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + dimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT8, memorySpaceId, dataSpaceId, H5P_DEFAULT,
                            data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset)
    {
        assert objectPath != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] memoryDimensions = data.longDimensions();
                    assert memoryDimensions.length == offset.length;
                    final long[] longBlockDimensions = MDArray.toLong(blockDimensions);
                    assert longBlockDimensions.length == offset.length;
                    final long[] dataSetDimensions = new long[blockDimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + blockDimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, longBlockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(memoryDimensions, registry);
                    baseWriter.h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset),
                            longBlockDimensions);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT8, memorySpaceId, dataSpaceId, H5P_DEFAULT,
                            data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }
}
