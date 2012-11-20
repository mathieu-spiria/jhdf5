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
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_STRING;

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
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

    @Override
    public String getStringAttribute(final String objectPath, final String attributeName)
    {
        return getStringAttribute(objectPath, attributeName, false);
    }

    @Override
    public String getStringAttributeFixedLength(final String objectPath, final String attributeName)
    {
        return getStringAttribute(objectPath, attributeName, true);
    }

    String getStringAttribute(final String objectPath, final String attributeName,
            final boolean readRaw)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                @Override
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.getStringAttribute(objectId, objectPath, attributeName,
                            readRaw, registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    @Override
    public int getStringAttributeExplicitLength(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> readRunnable = new ICallableWithCleanUp<Integer>()
            {
                @Override
                public Integer call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.getAttributeExplicitStringLength(objectId, attributeName,
                            registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    @Override
    public String[] getStringArrayAttribute(final String objectPath, final String attributeName)
    {
        return getStringArrayAttribute(objectPath, attributeName, false);
    }

    @Override
    public String[] getStringArrayAttributeFixedLength(final String objectPath,
            final String attributeName)
    {
        return getStringArrayAttribute(objectPath, attributeName, true);
    }

    String[] getStringArrayAttribute(final String objectPath, final String attributeName,
            final boolean readRaw)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> readRunnable = new ICallableWithCleanUp<String[]>()
            {
                @Override
                public String[] call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.getStringArrayAttribute(objectId, objectPath, attributeName,
                            readRaw, registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    @Override
    public MDArray<String> getStringMDArrayAttribute(final String objectPath,
            final String attributeName)
    {
        return getStringMDArrayAttribute(objectPath, attributeName, false);
    }

    @Override
    public MDArray<String> getStringMDArrayAttributeFixedLength(final String objectPath,
            final String attributeName)
    {
        return getStringMDArrayAttribute(objectPath, attributeName, true);
    }

    MDArray<String> getStringMDArrayAttribute(final String objectPath, final String attributeName,
            final boolean readRaw)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDArray<String>> readRunnable =
                new ICallableWithCleanUp<MDArray<String>>()
                    {
                        @Override
                        public MDArray<String> call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return baseReader.getStringMDArrayAttribute(objectId, objectPath,
                                    attributeName, readRaw, registry);
                        }
                    };
        return baseReader.runner.call(readRunnable);
    }

    //
    // Data Sets
    //

    @Override
    public String readString(final String objectPath) throws HDF5JavaException
    {
        return readString(objectPath, false);
    }

    String readString(final String objectPath, final boolean readRaw) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                @Override
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
                        final int length =
                                readRaw ? -1 : baseReader.getObjectExplicitStringLength(objectPath,
                                        registry);
                        return (length >= 0) ? StringUtils.fromBytes(data, length,
                                baseReader.encoding) : (readRaw ? StringUtils.fromBytes(data,
                                baseReader.encoding) : StringUtils.fromBytes0Term(data,
                                baseReader.encoding));
                    }
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    @Override
    public String[] readStringArray(final String objectPath) throws HDF5JavaException
    {
        return readStringArray(objectPath, false);
    }

    String[] readStringArray(final String objectPath, final boolean readRaw)
            throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                @Override
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId, registry);
                    final int oneDimSize = getOneDimensionalArraySize(dimensions);
                    final String[] data = new String[oneDimSize];
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
                        final int[] lengths =
                                readRaw ? null : baseReader.tryGetObjectArrayExplicitStringLength(
                                        objectPath, registry);
                        final int strLength = baseReader.h5.getDataTypeSize(dataTypeId);
                        byte[] bdata = null;
                        if (readRaw || lengths != null)
                        {
                            bdata = new byte[oneDimSize * strLength];
                            baseReader.h5.readDataSetNonNumeric(dataSetId, dataTypeId, bdata);
                        } else
                        {
                            baseReader.h5.readDataSetString(dataSetId, dataTypeId, data);
                        }
                        if (bdata != null)
                        {
                            if (lengths != null)
                            {
                                if (lengths.length != oneDimSize)
                                {
                                    throw new HDF5JavaException(
                                            "Size mismatch in string array: string array size = "
                                                    + data.length + ", length array size = "
                                                    + lengths.length);
                                }
                                for (int i = 0, startIdx = 0; i < oneDimSize; ++i, startIdx +=
                                        strLength)
                                {
                                    data[i] =
                                            StringUtils.fromBytes(bdata, startIdx, startIdx
                                                    + lengths[i], baseReader.encoding);
                                }
                            } else if (readRaw)
                            {
                                for (int i = 0, startIdx = 0; i < bdata.length; ++i, startIdx +=
                                        strLength)
                                {
                                    data[i] =
                                            StringUtils.fromBytes(bdata, startIdx, startIdx
                                                    + strLength, baseReader.encoding);
                                }
                            }
                        }
                    }
                    return data;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    @Override
    public String[] readStringArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readStringArrayBlockWithOffset(objectPath, blockSize, blockSize * blockNumber);
    }

    @Override
    public String[] readStringArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> readCallable = new ICallableWithCleanUp<String[]>()
            {
                @Override
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
                        // FIXME: explicit strings lengths.
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

    @Override
    public MDArray<String> readStringMDArray(final String objectPath)
    {
        return readStringMDArray(objectPath, false);
    }
    
    MDArray<String> readStringMDArray(final String objectPath, final boolean readRaw)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDArray<String>> readCallable =
                new ICallableWithCleanUp<MDArray<String>>()
                    {
                        @Override
                        public MDArray<String> call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final String[] data = new String[spaceParams.blockSize];
                            final int dataTypeId =
                                    baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                            if (baseReader.h5.isVariableLengthString(dataTypeId))
                            {
                                baseReader.h5.readDataSetVL(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        data);
                            } else
                            {
                                final boolean isString =
                                        (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                                if (isString == false)
                                {
                                    throw new HDF5JavaException(objectPath
                                            + " needs to be a String.");
                                }
                                
                                final MDIntArray lengths =
                                        readRaw ? null : baseReader.tryGetObjectMDArrayExplicitStringLength(
                                                objectPath, registry);
                                final int strLength = baseReader.h5.getDataTypeSize(dataTypeId);
                                byte[] bdata = null;
                                if (readRaw || lengths != null)
                                {
                                    bdata = new byte[spaceParams.blockSize * strLength];
                                    baseReader.h5.readDataSetNonNumeric(dataSetId, dataTypeId, bdata);
                                } else
                                {
                                    baseReader.h5.readDataSetString(dataSetId, dataTypeId, data);
                                }
                                if (bdata != null)
                                {
                                    if (lengths != null)
                                    {
                                        if (lengths.size() != spaceParams.blockSize)
                                        {
                                            throw new HDF5JavaException(
                                                    "Size mismatch in string array: string array size = "
                                                            + data.length + ", length array size = "
                                                            + lengths.size());
                                        }
                                        for (int i = 0, startIdx = 0; i < spaceParams.blockSize; ++i, startIdx +=
                                                strLength)
                                        {
                                            data[i] =
                                                    StringUtils.fromBytes(bdata, startIdx, startIdx
                                                            + lengths.getAsFlatArray()[i], baseReader.encoding);
                                        }
                                    } else if (readRaw)
                                    {
                                        for (int i = 0, startIdx = 0; i < bdata.length; ++i, startIdx +=
                                                strLength)
                                        {
                                            data[i] =
                                                    StringUtils.fromBytes(bdata, startIdx, startIdx
                                                            + strLength, baseReader.encoding);
                                        }
                                    }
                                }
                            }
                            return new MDArray<String>(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public MDArray<String> readStringMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDArray<String>> readCallable =
                new ICallableWithCleanUp<MDArray<String>>()
                    {
                        @Override
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
                                // FIXME: explicit strings lengths.
                                baseReader.h5.readDataSetString(dataSetId, dataTypeId,
                                        spaceParams.memorySpaceId, spaceParams.dataSpaceId,
                                        dataBlock);
                            }
                            return new MDArray<String>(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    @Override
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

    @Override
    public Iterable<HDF5DataBlock<String[]>> getStringArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(dataSetPath));

        return new Iterable<HDF5DataBlock<String[]>>()
            {
                @Override
                public Iterator<HDF5DataBlock<String[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<String[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5DataBlock<String[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final String[] block =
                                        readStringArrayBlockWithOffset(dataSetPath,
                                                index.getBlockSize(), offset);
                                return new HDF5DataBlock<String[]>(block, index.getAndIncIndex(),
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
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getStringMDArrayNaturalBlocks(
            final String objectPath)
    {
        baseReader.checkOpen();
        final HDF5NaturalBlockMDParameters params =
                new HDF5NaturalBlockMDParameters(baseReader.getDataSetInformation(objectPath));

        return new Iterable<HDF5MDDataBlock<MDArray<String>>>()
            {
                @Override
                public Iterator<HDF5MDDataBlock<MDArray<String>>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDArray<String>>>()
                        {
                            final HDF5NaturalBlockMDParameters.HDF5NaturalBlockMDIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5MDDataBlock<MDArray<String>> next()
                            {
                                final long[] offset = index.computeOffsetAndSizeGetOffsetClone();
                                final MDArray<String> data =
                                        readStringMDArrayBlockWithOffset(objectPath,
                                                index.getBlockSize(), offset);
                                return new HDF5MDDataBlock<MDArray<String>>(data,
                                        index.getIndexClone(), offset);
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
}
