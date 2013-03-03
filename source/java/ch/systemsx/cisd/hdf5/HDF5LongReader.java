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

import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_ARRAY;

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants;
import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;

/**
 * The implementation of {@link IHDF5LongReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5LongReader implements IHDF5LongReader
{
    private final HDF5BaseReader baseReader;

    HDF5LongReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    @Override
    public long getAttr(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> getAttributeRunnable = new ICallableWithCleanUp<Long>()
            {
                @Override
                public Long call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final long[] data =
                            baseReader.h5
                                    .readAttributeAsLongArray(attributeId, H5T_NATIVE_INT64, 1);
                    return data[0];
                }
            };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public long[] getArrayAttr(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> getAttributeRunnable =
                new ICallableWithCleanUp<long[]>()
                    {
                        @Override
                        public long[] call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return getLongArrayAttribute(objectId, attributeName, registry);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public MDLongArray getMDArrayAttr(final String objectPath,
            final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> getAttributeRunnable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        @Override
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return getLongMDArrayAttribute(objectId, attributeName, registry);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public long[][] getMatrixAttr(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        final MDLongArray array = getMDArrayAttr(objectPath, attributeName);
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
    public long read(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                @Override
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] data = new long[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                @Override
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    return readLongArray(dataSetId, registry);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    private long[] readLongArray(int dataSetId, ICleanUpRegistry registry)
    {
        try
        {
            final DataSpaceParameters spaceParams =
                    baseReader.getSpaceParameters(dataSetId, registry);
            final long[] data = new long[spaceParams.blockSize];
            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, spaceParams.memorySpaceId,
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
                    return readLongArrayFromArrayType(dataSetId, dataTypeId, registry);
                }
            }
            throw ex;
        }
    }

    private long[] readLongArrayFromArrayType(int dataSetId, final int dataTypeId,
            ICleanUpRegistry registry)
    {
        final int spaceId = baseReader.h5.createScalarDataSpace();
        final int[] dimensions = baseReader.h5.getArrayDimensions(dataTypeId);
        final long[] data = new long[HDF5Utils.getOneDimensionalArraySize(dimensions)];
        final int memoryDataTypeId =
                baseReader.h5.createArrayType(H5T_NATIVE_INT64, data.length, registry);
        baseReader.h5.readDataSet(dataSetId, memoryDataTypeId, spaceId, spaceId, data);
        return data;
    }

    @Override
    public int[] readToMDArrayWithOffset(final String objectPath, final MDLongArray array,
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
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT64, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array.
                            getAsFlatArray());
                    return MDArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public int[] readToMDArrayBlockWithOffset(final String objectPath,
            final MDLongArray array, final int[] blockDimensions, final long[] offset,
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
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT64, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                            .getAsFlatArray());
                    return MDArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readArrayBlockWithOffset(objectPath, blockSize, blockNumber * blockSize);
    }

    @Override
    public long[] readArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                @Override
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[][] readMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDLongArray array = readMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    @Override
    public long[][] readMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY) 
            throws HDF5JavaException
    {
        final MDLongArray array = readMDArrayBlock(objectPath, new int[]
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
    public long[][] readMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDLongArray array = readMDArrayBlockWithOffset(objectPath, new int[]
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
    public MDLongArray readMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable = new ICallableWithCleanUp<MDLongArray>()
            {
                @Override
                public MDLongArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    return readLongMDArray(dataSetId, registry);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    MDLongArray readLongMDArray(int dataSetId, ICleanUpRegistry registry)
    {
        try
        {
            final DataSpaceParameters spaceParams =
                    baseReader.getSpaceParameters(dataSetId, registry);
            final long[] data = new long[spaceParams.blockSize];
            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, spaceParams.memorySpaceId,
                    spaceParams.dataSpaceId, data);
            return new MDLongArray(data, spaceParams.dimensions);
        } catch (HDF5LibraryException ex)
        {
            if (ex.getMajorErrorNumber() == HDF5Constants.H5E_DATATYPE
                    && ex.getMinorErrorNumber() == HDF5Constants.H5E_CANTINIT)
            {
                // Check whether it is an array data type.
                final int dataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                if (baseReader.h5.getClassType(dataTypeId) == HDF5Constants.H5T_ARRAY)
                {
                    return readLongMDArrayFromArrayType(dataSetId, dataTypeId, registry);
                }
            }
            throw ex;
        }
    }

    private MDLongArray readLongMDArrayFromArrayType(int dataSetId, final int dataTypeId,
            ICleanUpRegistry registry)
    {
        final int spaceId = baseReader.h5.createScalarDataSpace();
        final int[] dimensions = baseReader.h5.getArrayDimensions(dataTypeId);
        final long[] data = new long[MDArray.getLength(dimensions)];
        final int memoryDataTypeId =
                baseReader.h5.createArrayType(H5T_NATIVE_INT64, dimensions, registry);
        baseReader.h5.readDataSet(dataSetId, memoryDataTypeId, spaceId, spaceId, data);
        return new MDLongArray(data, dimensions);
    }

    @Override
    public MDLongArray readMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * blockDimensions[i];
        }
        return readMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public MDLongArray readMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable = new ICallableWithCleanUp<MDLongArray>()
            {
                @Override
                public MDLongArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockDimensions, 
                                    registry);
                    final long[] dataBlock = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, dataBlock);
                    return new MDLongArray(dataBlock, blockDimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }
    
    @Override
    public Iterable<HDF5DataBlock<long[]>> getArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5DataBlock<long[]>>()
            {
                @Override
                public Iterator<HDF5DataBlock<long[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<long[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5DataBlock<long[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final long[] block =
                                        readArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5DataBlock<long[]>(block, index.getAndIncIndex(), 
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
    public Iterable<HDF5MDDataBlock<MDLongArray>> getMDArrayNaturalBlocks(final String dataSetPath)
    {
        baseReader.checkOpen();
        final HDF5NaturalBlockMDParameters params =
                new HDF5NaturalBlockMDParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5MDDataBlock<MDLongArray>>()
            {
                @Override
                public Iterator<HDF5MDDataBlock<MDLongArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDLongArray>>()
                        {
                            final HDF5NaturalBlockMDParameters.HDF5NaturalBlockMDIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5MDDataBlock<MDLongArray> next()
                            {
                                final long[] offset = index.computeOffsetAndSizeGetOffsetClone();
                                final MDLongArray data =
                                        readMDArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5MDDataBlock<MDLongArray>(data, index
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

    long[] getLongArrayAttribute(final int objectId, final String attributeName,
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
                    baseReader.h5.createArrayType(H5T_NATIVE_INT64, len,
                            registry);
        } else
        {
            final long[] arrayDimensions =
                    baseReader.h5.getDataDimensionsForAttribute(attributeId,
                            registry);
            memoryTypeId = H5T_NATIVE_INT64;
            len = HDF5Utils.getOneDimensionalArraySize(arrayDimensions);
        }
        final long[] data =
                baseReader.h5.readAttributeAsLongArray(attributeId,
                        memoryTypeId, len);
        return data;
    }

    MDLongArray getLongMDArrayAttribute(final int objectId,
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
                        baseReader.h5.createArrayType(H5T_NATIVE_INT64,
                                arrayDimensions, registry);
            } else
            {
                arrayDimensions =
                        MDArray.toInt(baseReader.h5.getDataDimensionsForAttribute(
                                attributeId, registry));
                memoryTypeId = H5T_NATIVE_INT64;
            }
            final int len = MDArray.getLength(arrayDimensions);
            final long[] data =
                    baseReader.h5.readAttributeAsLongArray(attributeId,
                            memoryTypeId, len);
            return new MDLongArray(data, arrayDimensions);
        } catch (IllegalArgumentException ex)
        {
            throw new HDF5JavaException(ex.getMessage());
        }
    }
}
