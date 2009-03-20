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


import static ch.systemsx.cisd.hdf5.HDF5.NO_DEFLATION;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_int;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.common.array.MDArray;
import ch.systemsx.cisd.common.array.MDIntArray;
import ch.systemsx.cisd.common.process.ICallableWithCleanUp;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5IntWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5IntWriter implements IHDF5IntWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5IntWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    public void writeInt(final String objectPath, final int value)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, H5T_STD_I32LE, H5T_NATIVE_INT32, HDFNativeData.
                intToByte(value));
    }

    public void createIntArrayCompact(final String objectPath, final long length)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I32LE, NO_DEFLATION, new long[]
                        { length }, null, true, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeIntArrayCompact(final String objectPath, final int[] data)
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
                            baseWriter.getDataSetId(objectPath, H5T_STD_I32LE, dimensions, 
                                    NO_DEFLATION, registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeIntArray(final String objectPath, final int[] data)
    {
        writeIntArray(objectPath, data, false);
    }

    public void writeIntArray(final String objectPath, final int[] data, final boolean deflate)
    {
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseWriter.getDataSetId(objectPath, H5T_STD_I32LE, new long[]
                        { data.length }, HDF5Utils.getDeflateLevel(deflate), registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createIntArray(final String objectPath, final long size, final int blockSize)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize >= 0 && blockSize <= size;

        baseWriter.checkOpen();
        createIntArray(objectPath, size, blockSize, false);
    }

    public void createIntArray(final String objectPath, final long size, final int blockSize,
            final boolean deflate)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize >= 0 && blockSize <= size;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I32LE, HDF5Utils
                            .getDeflateLevel(deflate), new long[]
                        { size }, new long[]
                        { blockSize }, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeIntArrayBlock(final String objectPath, final int[] data,
            final long blockNumber)
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
                    final long[] slabStartOrNull = new long[]
                        { data.length * blockNumber };
                    final int dataSetId = 
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, dimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeIntArrayBlockWithOffset(final String objectPath, final int[] data,
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>int</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeIntMatrix(final String objectPath, final int[][] data)
    {
        writeIntMatrix(objectPath, data, false);
    }

    public void writeIntMatrix(final String objectPath, final int[][] data, final boolean deflate)
    {
        assert objectPath != null;
        assert data != null;
        assert HDF5Utils.areMatrixDimensionsConsistent(data);

        writeIntMDArray(objectPath, new MDIntArray(data), deflate);
    }

    public void createIntMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY)
    {
        createIntMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, false);
    }

    public void createIntMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final boolean deflate)
    {
        assert objectPath != null;
        assert sizeX >= 0;
        assert sizeY >= 0;
        assert blockSizeX >= 0 && blockSizeX <= sizeX;
        assert blockSizeY >= 0 && blockSizeY <= sizeY;

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
                            .createDataSet(objectPath, H5T_STD_I32LE, HDF5Utils
                                    .getDeflateLevel(deflate), dimensions,
                            blockDimensions, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeIntMatrixBlock(final String objectPath, final int[][] data,
            final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;
        assert data != null;

        writeIntMDArrayBlock(objectPath, new MDIntArray(data), new long[]
            { blockNumberX, blockNumberY });
    }

    public void writeIntMatrixBlockWithOffset(final String objectPath, final int[][] data,
            final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeIntMDArrayBlockWithOffset(objectPath, new MDIntArray(data, new int[]
            { data.length, data[0].length }), new long[]
            { offsetX, offsetY });
    }

    public void writeIntMatrixBlockWithOffset(final String objectPath, final int[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeIntMDArrayBlockWithOffset(objectPath, new MDIntArray(data, new int[]
            { dataSizeX, dataSizeY }), new long[]
            { offsetX, offsetY });
    }

    public void writeIntMDArray(final String objectPath, final MDIntArray data)
    {
        writeIntMDArray(objectPath, data, false);
    }

    public void writeIntMDArray(final String objectPath, final MDIntArray data,
            final boolean deflate)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I32LE, 
                                    data.longDimensions(), HDF5Utils.getDeflateLevel(deflate), 
                                    registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, data
                            .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createIntMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions)
    {
        createIntMDArray(objectPath, dimensions, blockDimensions, false);
    }

    public void createIntMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final boolean deflate)
    {
        assert objectPath != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I32LE, HDF5Utils
                           .getDeflateLevel(deflate), dimensions, MDArray.toLong(blockDimensions), 
                           false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeIntMDArrayBlock(final String objectPath, final MDIntArray data,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert data != null;
        assert blockNumber != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] dimensions = data.longDimensions();
                    assert dimensions.length == blockNumber.length;
                    final long[] offset = new long[dimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        offset[i] = blockNumber[i] * dimensions[i];
                    }
                    final int dataSetId = 
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray data,
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
                    final int dataSetId = 
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray data,
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
                    final int dataSetId = 
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    final int dataSpaceId = 
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, longBlockDimensions);
                    final int memorySpaceId = 
                            baseWriter.h5.createSimpleDataSpace(memoryDimensions, registry);
                    baseWriter.h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset),
                            longBlockDimensions);
                    H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }
}
