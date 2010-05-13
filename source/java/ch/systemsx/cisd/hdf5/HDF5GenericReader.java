/*
 * Copyright 2010 ETH Zuerich, CISD
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

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * Implementation of {@link IHDF5GenericReader}
 * 
 * @author Bernd Rinn
 */
public class HDF5GenericReader implements IHDF5GenericReader
{

    private final HDF5BaseReader baseReader;

    HDF5GenericReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    public String tryGetOpaqueTag(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readTagCallable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int dataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    return baseReader.h5.tryGetOpaqueTag(dataTypeId);
                }
            };
        return baseReader.runner.call(readTagCallable);
    }

    public HDF5OpaqueType tryGetOpaqueType(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5OpaqueType> readTagCallable =
                new ICallableWithCleanUp<HDF5OpaqueType>()
                    {
                        public HDF5OpaqueType call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int dataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId,
                                            baseReader.fileRegistry);
                            final String opaqueTagOrNull =
                                    baseReader.h5.tryGetOpaqueTag(dataTypeId);
                            if (opaqueTagOrNull == null)
                            {
                                return null;
                            } else
                            {
                                return new HDF5OpaqueType(baseReader.fileId, dataTypeId,
                                        opaqueTagOrNull);
                            }
                        }
                    };
        return baseReader.runner.call(readTagCallable);
    }

    public byte[] readAsByteArray(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSetCheckBitFields(dataSetId,
                                    registry);
                    final int elementSize = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    final byte[] data =
                            new byte[((spaceParams.blockSize == 0) ? 1 : spaceParams.blockSize)
                                    * elementSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public byte[] readAsByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final int elementSize = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    final byte[] data = new byte[elementSize * spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public byte[] readAsByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final int elementSize = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    final byte[] data = new byte[elementSize * spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public int readAsByteArrayToBlockWithOffset(final String objectPath, final byte[] buffer,
            final int blockSize, final long offset, final int memoryOffset)
            throws HDF5JavaException
    {
        if (blockSize + memoryOffset > buffer.length)
        {
            throw new HDF5JavaException("Buffer not large enough for blockSize and memoryOffset");
        }
        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> readCallable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, memoryOffset, offset,
                                    blockSize, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final int elementSize = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    if ((blockSize + memoryOffset) * elementSize > buffer.length)
                    {
                        throw new HDF5JavaException(
                                "Buffer not large enough for blockSize and memoryOffset");
                    }
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, buffer);
                    return spaceParams.blockSize;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public Iterable<HDF5DataBlock<byte[]>> getAsByteArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5DataBlock<byte[]>>()
            {
                public Iterator<HDF5DataBlock<byte[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<byte[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            public HDF5DataBlock<byte[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final byte[] block =
                                        readAsByteArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5DataBlock<byte[]>(block, index.getAndIncIndex(),
                                        offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

}
