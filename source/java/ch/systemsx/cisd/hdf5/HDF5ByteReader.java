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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;

import java.util.Iterator;

import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import ch.systemsx.cisd.base.mdarray.MDAbstractArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5ByteReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5ByteReader implements IHDF5ByteReader
{
    private final HDF5BaseReader baseReader;

    HDF5ByteReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    @Override
    public byte getByteAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Byte> getAttributeRunnable = new ICallableWithCleanUp<Byte>()
            {
                @Override
                public Byte call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, H5T_NATIVE_INT8, 1);
                    return data[0];
                }
            };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public byte[] getByteArrayAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> getAttributeRunnable =
                new ICallableWithCleanUp<byte[]>()
                    {
                        @Override
                        public byte[] call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return getByteArrayAttribute(objectId, attributeName, registry);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public MDByteArray getByteMDArrayAttribute(final String objectPath,
            final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> getAttributeRunnable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        @Override
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return getByteMDArrayAttribute(objectId, attributeName, registry);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public byte[][] getByteMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        final MDByteArray array = getByteMDArrayAttribute(objectPath, attributeName);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    @Override
    public byte readByte(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Byte> readCallable = new ICallableWithCleanUp<Byte>()
            {
                @Override
                public Byte call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final byte[] data = new byte[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public byte[] readByteArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                @Override
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    return readByteArray(dataSetId, registry);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    private byte[] readByteArray(int dataSetId, ICleanUpRegistry registry)
    {
        try
        {
            final DataSpaceParameters spaceParams =
                    baseReader.getSpaceParameters(dataSetId, registry);
            final byte[] data = new byte[spaceParams.blockSize];
            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, spaceParams.memorySpaceId,
                    spaceParams.dataSpaceId, data);
            return data;
        } catch (HDF5LibraryException ex)
        {
            if (ex.getMajorErrorNumber() == HDF5Constants.H5E_DATATYPE
                    && ex.getMinorErrorNumber() == HDF5Constants.H5E_CANTINIT)
            {
                // Check whether it is an array data type.
                final int dataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                if (baseReader.h5.getClassType(dataTypeId) == HDF5Constants.H5T_ARRAY)
                {
                    return readByteArrayFromArrayType(dataSetId, dataTypeId, registry);
                }
            }
            throw ex;
        }
    }

    private byte[] readByteArrayFromArrayType(int dataSetId, final int dataTypeId,
            ICleanUpRegistry registry)
    {
        final int spaceId = baseReader.h5.createScalarDataSpace();
        final int[] dimensions = baseReader.h5.getArrayDimensions(dataTypeId);
        final byte[] data = new byte[HDF5Utils.getOneDimensionalArraySize(dimensions)];
        final int memoryDataTypeId =
                baseReader.h5.createArrayType(H5T_NATIVE_INT8, data.length, registry);
        baseReader.h5.readDataSet(dataSetId, memoryDataTypeId, spaceId, spaceId, data);
        return data;
    }

    @Override
    public int[] readToByteMDArrayWithOffset(final String objectPath, final MDByteArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                @Override
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT8, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array.
                            getAsFlatArray());
                    return MDAbstractArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public int[] readToByteMDArrayBlockWithOffset(final String objectPath,
            final MDByteArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                @Override
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT8, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                            .getAsFlatArray());
                    return MDAbstractArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public byte[] readByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readByteArrayBlockWithOffset(objectPath, blockSize, blockNumber * blockSize);
    }

    @Override
    public byte[] readByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                @Override
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public byte[][] readByteMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    @Override
    public byte[][] readByteMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY) 
            throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    @Override
    public byte[][] readByteMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    @Override
    public MDByteArray readByteMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable = new ICallableWithCleanUp<MDByteArray>()
            {
                @Override
                public MDByteArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    return readByteMDArray(dataSetId, registry);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    private MDByteArray readByteMDArray(int dataSetId, ICleanUpRegistry registry)
    {
        try
        {
            final DataSpaceParameters spaceParams =
                    baseReader.getSpaceParameters(dataSetId, registry);
            final byte[] data = new byte[spaceParams.blockSize];
            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, spaceParams.memorySpaceId,
                    spaceParams.dataSpaceId, data);
            return new MDByteArray(data, spaceParams.dimensions);
        } catch (HDF5LibraryException ex)
        {
            if (ex.getMajorErrorNumber() == HDF5Constants.H5E_DATATYPE
                    && ex.getMinorErrorNumber() == HDF5Constants.H5E_CANTINIT)
            {
                // Check whether it is an array data type.
                final int dataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                if (baseReader.h5.getClassType(dataTypeId) == HDF5Constants.H5T_ARRAY)
                {
                    return readByteMDArrayFromArrayType(dataSetId, dataTypeId, registry);
                }
            }
            throw ex;
        }
    }

    private MDByteArray readByteMDArrayFromArrayType(int dataSetId, final int dataTypeId,
            ICleanUpRegistry registry)
    {
        final int spaceId = baseReader.h5.createScalarDataSpace();
        final int[] dimensions = baseReader.h5.getArrayDimensions(dataTypeId);
        final byte[] data = new byte[MDAbstractArray.getLength(dimensions)];
        final int memoryDataTypeId =
                baseReader.h5.createArrayType(H5T_NATIVE_INT8, dimensions, registry);
        baseReader.h5.readDataSet(dataSetId, memoryDataTypeId, spaceId, spaceId, data);
        return new MDByteArray(data, dimensions);
    }

    @Override
    public MDByteArray readByteMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * blockDimensions[i];
        }
        return readByteMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public MDByteArray readByteMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable = new ICallableWithCleanUp<MDByteArray>()
            {
                @Override
                public MDByteArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockDimensions, 
                                    registry);
                    final byte[] dataBlock = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, dataBlock);
                    return new MDByteArray(dataBlock, blockDimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }
    
    @Override
    public Iterable<HDF5DataBlock<byte[]>> getByteArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5DataBlock<byte[]>>()
            {
                @Override
                public Iterator<HDF5DataBlock<byte[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<byte[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5DataBlock<byte[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final byte[] block =
                                        readByteArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5DataBlock<byte[]>(block, index.getAndIncIndex(), 
                                        offset);
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDByteArray>> getByteMDArrayNaturalBlocks(final String dataSetPath)
    {
        baseReader.checkOpen();
        final HDF5NaturalBlockMDParameters params =
                new HDF5NaturalBlockMDParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5MDDataBlock<MDByteArray>>()
            {
                @Override
                public Iterator<HDF5MDDataBlock<MDByteArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDByteArray>>()
                        {
                            final HDF5NaturalBlockMDParameters.HDF5NaturalBlockMDIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5MDDataBlock<MDByteArray> next()
                            {
                                final long[] offset = index.computeOffsetAndSizeGetOffsetClone();
                                final MDByteArray data =
                                        readByteMDArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5MDDataBlock<MDByteArray>(data, index
                                        .getIndexClone(), offset);
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    byte[] getByteArrayAttribute(final int objectId, final String attributeName,
            ICleanUpRegistry registry)
    {
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
                    baseReader.h5.createArrayType(H5T_NATIVE_INT8, len,
                            registry);
        } else
        {
            final long[] arrayDimensions =
                    baseReader.h5.getDataDimensionsForAttribute(attributeId,
                            registry);
            memoryTypeId = H5T_NATIVE_INT8;
            len = HDF5Utils.getOneDimensionalArraySize(arrayDimensions);
        }
        final byte[] data =
                baseReader.h5.readAttributeAsByteArray(attributeId,
                        memoryTypeId, len);
        return data;
    }

    MDByteArray getByteMDArrayAttribute(final int objectId,
            final String attributeName, ICleanUpRegistry registry)
    {
        try
        {
            final int attributeId =
                    baseReader.h5.openAttribute(objectId, attributeName, registry);
            final int attributeTypeId =
                    baseReader.h5.getDataTypeForAttribute(attributeId, registry);
            final int memoryTypeId;
            final int[] arrayDimensions;
            if (baseReader.h5.getClassType(attributeTypeId) == H5T_ARRAY)
            {
                arrayDimensions = baseReader.h5.getArrayDimensions(attributeTypeId);
                memoryTypeId =
                        baseReader.h5.createArrayType(H5T_NATIVE_INT8,
                                arrayDimensions, registry);
            } else
            {
                arrayDimensions =
                        MDAbstractArray.toInt(baseReader.h5.getDataDimensionsForAttribute(
                                attributeId, registry));
                memoryTypeId = H5T_NATIVE_INT8;
            }
            final int len = MDAbstractArray.getLength(arrayDimensions);
            final byte[] data =
                    baseReader.h5.readAttributeAsByteArray(attributeId,
                            memoryTypeId, len);
            return new MDByteArray(data, arrayDimensions);
        } catch (IllegalArgumentException ex)
        {
            throw new HDF5JavaException(ex.getMessage());
        }
    }
}
