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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;

import java.util.Iterator;
import java.util.NoSuchElementException;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;

/**
 * The implementation of {@link IHDF5IntReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5IntReader implements IHDF5IntReader
{
    private final HDF5BaseReader baseReader;

    HDF5IntReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public int getIntAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> getAttributeRunnable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, H5T_NATIVE_INT32, 4);
                    return HDFNativeData.byteToInt(data, 0);
                }
            };
        return baseReader.runner.call(getAttributeRunnable);
    }

    public int[] getIntArrayAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> getAttributeRunnable =
                new ICallableWithCleanUp<int[]>()
                    {
                        public int[] call(ICleanUpRegistry registry)
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
                                        baseReader.h5.createArrayType(H5T_NATIVE_INT32, len,
                                                registry);
                            } else
                            {
                                final long[] arrayDimensions =
                                        baseReader.h5.getDataDimensionsForAttribute(attributeId,
                                                registry);
                                memoryTypeId = H5T_NATIVE_INT32;
                                len = HDF5Utils.getOneDimensionalArraySize(arrayDimensions);
                            }
                            final byte[] data =
                                    baseReader.h5.readAttributeAsByteArray(attributeId,
                                            memoryTypeId, 4 * len);
                            return HDFNativeData.byteToInt(data, 0, len);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    public MDIntArray getIntMDArrayAttribute(final String objectPath,
            final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> getAttributeRunnable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final long[] arrayDimensions =
                                    baseReader.h5.getDataDimensionsForAttribute(attributeId,
                                            registry);
                            final int len;
                            try
                            {
                                len = MDArray.getLength(arrayDimensions);
                            } catch (IllegalArgumentException ex)
                            {
                                throw new HDF5JavaException(ex.getMessage());
                            }
                            final byte[] data =
                                    baseReader.h5.readAttributeAsByteArray(attributeId,
                                            H5T_NATIVE_INT32, 4 * len);
                            return new MDIntArray(HDFNativeData.byteToInt(data, 0, len),
                                    arrayDimensions);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    public int[][] getIntMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        final MDIntArray array = getIntMDArrayAttribute(objectPath, attributeName);
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

    public int readInt(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> readCallable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int[] data = new int[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int[] readIntArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams = 
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int[] readToIntMDArrayWithOffset(final String objectPath, final MDIntArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array.
                            getAsFlatArray());
                    return MDArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int[] readToIntMDArrayBlockWithOffset(final String objectPath,
            final MDIntArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, 
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                            .getAsFlatArray());
                    return MDArray.toInt(spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int[] readIntArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readIntArrayBlockWithOffset(objectPath, blockSize, blockNumber * blockSize);
    }

    public int[] readIntArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int[][] readIntMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    public int[][] readIntMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY) 
            throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    public int[][] readIntMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    public MDIntArray readIntMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable = new ICallableWithCleanUp<MDIntArray>()
            {
                public MDIntArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return new MDIntArray(data, spaceParams.dimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public MDIntArray readIntMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * blockDimensions[i];
        }
        return readIntMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public MDIntArray readIntMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable = new ICallableWithCleanUp<MDIntArray>()
            {
                public MDIntArray call(ICleanUpRegistry registry)
                {
                    final int dataSetId = 
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockDimensions, 
                                    registry);
                    final int[] dataBlock = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, dataBlock);
                    return new MDIntArray(dataBlock, blockDimensions);
                }
            };
        return baseReader.runner.call(readCallable);
    }
    
    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5DataSetInformation info = baseReader.getDataSetInformation(dataSetPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<int[]>>()
            {
                public Iterator<HDF5DataBlock<int[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<int[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<int[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final int[] block =
                                        readIntArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<int[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(final String dataSetPath)
    {
        baseReader.checkOpen();
        final HDF5DataSetInformation info = baseReader.getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDIntArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDIntArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDIntArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDIntArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDIntArray data =
                                        readIntMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDIntArray>(data, index.clone(),
                                        offset.clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }
}
