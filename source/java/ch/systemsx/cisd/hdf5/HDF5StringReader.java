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

import static ch.systemsx.cisd.hdf5.HDF5Utils.getOneDimensionalArraySize;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STRING;

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5StringReader}.
 * 
 * @author Bernd Rinn
 */
public class HDF5StringReader implements IHDF5StringReader
{

    private final HDF5BaseReader baseReader;

    HDF5StringReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    //
    // Attributes
    //

    public String getStringAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.getStringAttribute(objectId, objectPath, attributeName,
                            registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    public String[] getStringArrayAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> readRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.getStringArrayAttribute(objectId, objectPath, attributeName,
                            registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    //
    // Data Sets
    //

    public String readString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int dataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final boolean isString = (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a String.");
                    }
                    if (baseReader.h5.isVariableLengthString(dataTypeId))
                    {
                        String[] data = new String[1];
                        baseReader.h5.readDataSetVL(dataSetId, dataTypeId, data);
                        return data[0];
                    } else
                    {
                        final int size = baseReader.h5.getDataTypeSize(dataTypeId);
                        byte[] data = new byte[size];
                        baseReader.h5.readDataSetNonNumeric(dataSetId, dataTypeId, data);
                        int termIdx;
                        for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
                        {
                        }
                        return new String(data, 0, termIdx);
                    }
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    public String[] readStringArray(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                    final String[] data = new String[getOneDimensionalArraySize(dimensions)];
                    final int dataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    if (baseReader.h5.isVariableLengthString(dataTypeId))
                    {
                        baseReader.h5.readDataSetVL(dataSetId, dataTypeId, data);
                    } else
                    {
                        final boolean isString =
                                (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                        if (isString == false)
                        {
                            throw new HDF5JavaException(objectPath + " needs to be a String.");
                        }
                        baseReader.h5.readDataSetString(dataSetId, dataTypeId, data);
                    }
                    return data;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    public String[] readStringArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readStringArrayBlockWithOffset(objectPath, blockSize, blockSize * blockNumber);
    }

    public String[] readStringArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> readCallable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final String[] data = new String[spaceParams.blockSize];
                    final int dataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    if (baseReader.h5.isVariableLengthString(dataTypeId))
                    {
                        baseReader.h5.readDataSetVL(dataSetId, dataTypeId,
                                spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    } else
                    {
                        final boolean isString =
                                (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                        if (isString == false)
                        {
                            throw new HDF5JavaException(objectPath + " needs to be a String.");
                        }
                        baseReader.h5.readDataSetString(dataSetId, dataTypeId,
                                spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    }
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    public MDArray<String> readStringMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDArray<String>> readCallable =
                new ICallableWithCleanUp<MDArray<String>>()
                    {
                        public MDArray<String> call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final String[] dataBlock = new String[spaceParams.blockSize];
                            final int dataTypeId =
                                    baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                            if (baseReader.h5.isVariableLengthString(dataTypeId))
                            {
                                baseReader.h5.readDataSetVL(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        dataBlock);
                            } else
                            {
                                final boolean isString =
                                        (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                                if (isString == false)
                                {
                                    throw new HDF5JavaException(objectPath
                                            + " needs to be a String.");
                                }
                                baseReader.h5.readDataSetString(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        dataBlock);
                            }
                            return new MDArray<String>(dataBlock, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    public MDArray<String> readStringMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDArray<String>> readCallable =
                new ICallableWithCleanUp<MDArray<String>>()
                    {
                        public MDArray<String> call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final String[] dataBlock = new String[spaceParams.blockSize];
                            final int dataTypeId =
                                    baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                            if (baseReader.h5.isVariableLengthString(dataTypeId))
                            {
                                baseReader.h5.readDataSetVL(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        dataBlock);
                            } else
                            {
                                final boolean isString =
                                        (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                                if (isString == false)
                                {
                                    throw new HDF5JavaException(objectPath
                                            + " needs to be a String.");
                                }
                                baseReader.h5.readDataSetString(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        dataBlock);
                            }
                            return new MDArray<String>(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    public MDArray<String> readStringMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber)
    {
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockNumber[i] * blockDimensions[i];
        }
        return readStringMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public Iterable<HDF5DataBlock<String[]>> getStringArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5DataBlock<String[]>>()
            {
                public Iterator<HDF5DataBlock<String[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<String[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            public HDF5DataBlock<String[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final String[] block =
                                        readStringArrayBlockWithOffset(dataSetPath, index
                                                .getBlockSize(), offset);
                                return new HDF5DataBlock<String[]>(block, index.getAndIncIndex(),
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

    public Iterable<HDF5MDDataBlock<MDArray<String>>> getStringMDArrayNaturalBlocks(
            final String objectPath)
    {
        baseReader.checkOpen();
        final HDF5NaturalBlockMDParameters params =
                new HDF5NaturalBlockMDParameters(baseReader.getDataSetInformation(objectPath));

        return new Iterable<HDF5MDDataBlock<MDArray<String>>>()
            {
                public Iterator<HDF5MDDataBlock<MDArray<String>>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDArray<String>>>()
                        {
                            final HDF5NaturalBlockMDParameters.HDF5NaturalBlockMDIndex index =
                                    params.getNaturalBlockIndex();

                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            public HDF5MDDataBlock<MDArray<String>> next()
                            {
                                final long[] offset = index.computeOffsetAndSizeGetOffsetClone();
                                final MDArray<String> data =
                                        readStringMDArrayBlockWithOffset(objectPath, index
                                                .getBlockSize(), offset);
                                return new HDF5MDDataBlock<MDArray<String>>(data, index
                                        .getIndexClone(), offset);
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
