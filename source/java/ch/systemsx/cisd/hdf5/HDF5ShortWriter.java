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


import static ch.systemsx.cisd.hdf5.HDF5IntCompression.INT_NO_COMPRESSION;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_short;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I16LE;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5ShortWriter}.
 * 
 * @author Bernd Rinn
 */
class HDF5ShortWriter implements IHDF5ShortWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5ShortWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setShortAttribute(final String objectPath, final String name, final short value)
    {
        assert objectPath != null;
        assert name != null;

        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, H5T_STD_I16LE, H5T_NATIVE_INT16, HDFNativeData
                .shortToByte(value));
    }

    public void setShortArrayAttribute(final String objectPath, final String name,
            final short[] value)
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
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT16, value.length, registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I16LE, value.length, registry);
                    baseWriter.setAttribute(objectPath, name, storageTypeId, memoryTypeId,
                            HDFNativeData.shortToByte(value));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    public void setShortMDArrayAttribute(final String objectPath, final String name,
            final MDShortArray value)
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
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT16, value.dimensions(),
                                    registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I16LE, value.dimensions(),
                                    registry);
                    baseWriter.setAttribute(objectPath, name, storageTypeId, memoryTypeId,
                            HDFNativeData.shortToByte(value.getAsFlatArray()));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(addAttributeRunnable);
    }

    public void setShortMatrixAttribute(final String objectPath, final String name,
            final short[][] value)
    {
        setShortMDArrayAttribute(objectPath, name, new MDShortArray(value));
    }
    
    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeShort(final String objectPath, final short value)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, H5T_STD_I16LE, H5T_NATIVE_INT16, HDFNativeData.
                shortToByte(value));
    }

    public void createShortArrayCompact(final String objectPath, final long length)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I16LE, INT_NO_COMPRESSION, new long[]
                        { length }, null, true, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeShortArrayCompact(final String objectPath, final short[] data)
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
                            baseWriter.getDataSetId(objectPath, H5T_STD_I16LE, dimensions, 
                                    INT_NO_COMPRESSION, true, registry);
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeShortArray(final String objectPath, final short[] data)
    {
        writeShortArray(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeShortArray(final String objectPath, final short[] data,
            final HDF5IntCompression compression)
    {
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I16LE, new long[]
                                { data.length }, compression, false, registry);
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createShortArray(final String objectPath, final int blockSize)
    {
        createShortArray(objectPath, 0, blockSize, INT_NO_COMPRESSION);
    }

    public void createShortArray(final String objectPath, final long size, final int blockSize)
    {
        createShortArray(objectPath, size, blockSize, INT_NO_COMPRESSION);
    }

    public void createShortArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntCompression compression)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I16LE, compression, new long[]
                        { size }, new long[]
                        { blockSize }, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeShortArrayBlock(final String objectPath, final short[] data,
            final long blockNumber)
    {
        writeShortArrayBlockWithOffset(objectPath, data, data.length, data.length * blockNumber);
    }

    public void writeShortArrayBlockWithOffset(final String objectPath, final short[] data,
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
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>short</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeShortMatrix(final String objectPath, final short[][] data)
    {
        writeShortMatrix(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeShortMatrix(final String objectPath, final short[][] data, 
            final HDF5IntCompression compression)
    {
        assert objectPath != null;
        assert data != null;
        assert HDF5Utils.areMatrixDimensionsConsistent(data);

        writeShortMDArray(objectPath, new MDShortArray(data), compression);
    }

    public void createShortMatrix(final String objectPath, final int blockSizeX, 
            final int blockSizeY)
    {
        createShortMatrix(objectPath, 0, 0, blockSizeX, blockSizeY, INT_NO_COMPRESSION);
    }

    public void createShortMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY)
    {
        createShortMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, INT_NO_COMPRESSION);
    }

    public void createShortMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntCompression compression)
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
                            .createDataSet(objectPath, H5T_STD_I16LE, compression, dimensions,
                            blockDimensions, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeShortMatrixBlock(final String objectPath, final short[][] data,
            final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;
        assert data != null;

        writeShortMDArrayBlock(objectPath, new MDShortArray(data), new long[]
            { blockNumberX, blockNumberY });
    }

    public void writeShortMatrixBlockWithOffset(final String objectPath, final short[][] data,
            final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeShortMDArrayBlockWithOffset(objectPath, new MDShortArray(data, new int[]
            { data.length, data[0].length }), new long[]
            { offsetX, offsetY });
    }

    public void writeShortMatrixBlockWithOffset(final String objectPath, final short[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;
        assert data != null;

        writeShortMDArrayBlockWithOffset(objectPath, new MDShortArray(data, new int[]
            { dataSizeX, dataSizeY }), new long[]
            { offsetX, offsetY });
    }

    public void writeShortMDArray(final String objectPath, final MDShortArray data)
    {
        writeShortMDArray(objectPath, data, INT_NO_COMPRESSION);
    }

    public void writeShortMDArray(final String objectPath, final MDShortArray data,
            final HDF5IntCompression compression)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I16LE, 
                                    data.longDimensions(), compression, false, registry);
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, 
                            data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createShortMDArray(final String objectPath, final int[] blockDimensions)
    {
        createShortMDArray(objectPath, new long[blockDimensions.length], blockDimensions, INT_NO_COMPRESSION);
    }

    public void createShortMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions)
    {
        createShortMDArray(objectPath, dimensions, blockDimensions, INT_NO_COMPRESSION);
    }

    public void createShortMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntCompression compression)
    {
        assert objectPath != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, H5T_STD_I16LE, compression, dimensions, 
                            MDArray.toLong(blockDimensions), false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeShortMDArrayBlock(final String objectPath, final MDShortArray data,
            final long[] blockNumber)
    {
        assert blockNumber != null;

        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * dimensions[i];
        }
        writeShortMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeShortMDArrayBlockWithOffset(final String objectPath, final MDShortArray data,
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
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, memorySpaceId, dataSpaceId, 
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeShortMDArrayBlockWithOffset(final String objectPath, final MDShortArray data,
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
                    H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }
}
