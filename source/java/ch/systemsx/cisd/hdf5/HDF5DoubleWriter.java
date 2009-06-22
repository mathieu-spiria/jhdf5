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


import static ch.systemsx.cisd.hdf5.HDF5FloatCompression.FLOAT_NO_COMPRESSION;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_double;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_DOUBLE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F64LE;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5DoubleWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5DoubleWriter implements IHDF5DoubleWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5DoubleWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setDoubleAttribute(final String objectPath, final String name, final double value)
    {
        assert objectPath != null;
        assert name != null;

        baseWriter.checkOpen();
        baseWriter.addAttribute(objectPath, name, H5T_IEEE_F64LE, H5T_NATIVE_DOUBLE, HDFNativeData
                .doubleToByte(value));
    }

    public void setDoubleArrayAttribute(final String objectPath, final String name,
            final double[] value)
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
                            baseWriter.h5.createArrayType(H5T_NATIVE_DOUBLE, value.length, registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_IEEE_F64LE, value.length, registry);
                    baseWriter.addAttribute(objectPath, name, storageTypeId, memoryTypeId,
                            HDFNativeData.doubleToByte(value));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(addAttributeRunnable);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeDouble(final String objectPath, final double value)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, H5T_IEEE_F64LE, H5T_NATIVE_DOUBLE, HDFNativeData.
                doubleToByte(value));
    }

    public void createDoubleArrayCompact(final String objectPath, final long length)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_IEEE_F64LE, FLOAT_NO_COMPRESSION, new long[]
                        { length }, null, true, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeDoubleArrayCompact(final String objectPath, final double[] data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = new long[]
                        { data.length };
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_IEEE_F64LE, dimensions, 
                                    FLOAT_NO_COMPRESSION, true, true, registry);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeDoubleArray(final String objectPath, final double[] data)
    {
        writeDoubleArray(objectPath, data, FLOAT_NO_COMPRESSION);
    }

    public void writeDoubleArray(final String objectPath, final double[] data,
            final HDF5FloatCompression compression)
    {
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_IEEE_F64LE, new long[]
                                { data.length }, compression, true, false, registry);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createDoubleArray(final String objectPath, final int blockSize)
    {
        createDoubleArray(objectPath, 0, blockSize, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleArray(final String objectPath, final long size, final int blockSize)
    {
        createDoubleArray(objectPath, size, blockSize, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleArray(final String objectPath, final long size, final int blockSize,
            final HDF5FloatCompression compression)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_IEEE_F64LE, compression, new long[]
                        { size }, new long[]
                        { blockSize }, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeDoubleArrayBlock(final String objectPath, final double[] data,
            final long blockNumber)
    {
        writeDoubleArrayBlockWithOffset(objectPath, data, data.length, data.length * blockNumber);
    }

    public void writeDoubleArrayBlockWithOffset(final String objectPath, final double[] data,
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
                                        { offset + dataSize }, false, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>double</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeDoubleMatrix(final String objectPath, final double[][] data)
    {
        writeDoubleMatrix(objectPath, data, FLOAT_NO_COMPRESSION);
    }

    public void writeDoubleMatrix(final String objectPath, final double[][] data, 
            final HDF5FloatCompression compression)
    {
        assert objectPath != null;
        assert data != null;
        assert HDF5Utils.areMatrixDimensionsConsistent(data);

        writeDoubleMDArray(objectPath, new MDDoubleArray(data), compression);
    }

    public void createDoubleMatrix(final String objectPath, final int blockSizeX, 
            final int blockSizeY)
    {
        createDoubleMatrix(objectPath, 0, 0, blockSizeX, blockSizeY, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY)
    {
        createDoubleMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5FloatCompression compression)
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
                    baseWriter
                            .createDataSet(objectPath, H5T_IEEE_F64LE, compression, dimensions,
                            blockDimensions, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeDoubleMatrixBlock(final String objectPath, final double[][] data,
            final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;
        assert data != null;

        writeDoubleMDArrayBlock(objectPath, new MDDoubleArray(data), new long[]
            { blockNumberX, blockNumberY });
    }

    public void writeDoubleMatrixBlockWithOffset(final String objectPath, final double[][] data,
            final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeDoubleMDArrayBlockWithOffset(objectPath, new MDDoubleArray(data, new int[]
            { data.length, data[0].length }), new long[]
            { offsetX, offsetY });
    }

    public void writeDoubleMatrixBlockWithOffset(final String objectPath, final double[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeDoubleMDArrayBlockWithOffset(objectPath, new MDDoubleArray(data, new int[]
            { dataSizeX, dataSizeY }), new long[]
            { offsetX, offsetY });
    }

    public void writeDoubleMDArray(final String objectPath, final MDDoubleArray data)
    {
        writeDoubleMDArray(objectPath, data, FLOAT_NO_COMPRESSION);
    }

    public void writeDoubleMDArray(final String objectPath, final MDDoubleArray data,
            final HDF5FloatCompression compression)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_IEEE_F64LE, 
                                    data.longDimensions(), compression, true,
                                    false, registry);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createDoubleMDArray(final String objectPath, final int[] blockDimensions)
    {
        createDoubleMDArray(objectPath, new long[blockDimensions.length], blockDimensions, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions)
    {
        createDoubleMDArray(objectPath, dimensions, blockDimensions, FLOAT_NO_COMPRESSION);
    }

    public void createDoubleMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5FloatCompression compression)
    {
        assert objectPath != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_IEEE_F64LE, compression, dimensions, 
                            MDArray.toLong(blockDimensions), false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeDoubleMDArrayBlock(final String objectPath, final MDDoubleArray data,
            final long[] blockNumber)
    {
        assert blockNumber != null;

        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
        }
        writeDoubleMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeDoubleMDArrayBlockWithOffset(final String objectPath, final MDDoubleArray data,
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
                                    baseWriter.fileFormat, dataSetDimensions, false, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeDoubleMDArrayBlockWithOffset(final String objectPath, final MDDoubleArray data,
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
                                    baseWriter.fileFormat, dataSetDimensions, false, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, longBlockDimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(memoryDimensions, registry);
                    baseWriter.h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset),
                            longBlockDimensions);
                    H5Dwrite_double(dataSetId, H5T_NATIVE_DOUBLE, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }
}
