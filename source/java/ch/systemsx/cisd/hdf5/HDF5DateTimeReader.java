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

import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;

import java.util.Date;
import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * The implementation of {@link IHDF5DateTimeReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5DateTimeReader implements IHDF5DateTimeReader
{

    private final HDF5BaseReader baseReader;

    HDF5DateTimeReader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
    }

    @Override
    public long getTimeStampAttribute(final String objectPath, final String attributeName)
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
                    baseReader.checkIsTimeStamp(objectPath, attributeName, objectId, registry);
                    final long[] data =
                            baseReader.h5
                                    .readAttributeAsLongArray(attributeId, H5T_NATIVE_INT64, 1);
                    return data[0];
                }
            };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public Date getDateAttribute(String objectPath, String attributeName)
    {
        return new Date(getTimeStampAttribute(objectPath, attributeName));
    }

    @Override
    public long[] getTimeStampArrayAttribute(final String objectPath, final String attributeName)
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
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            baseReader.checkIsTimeStamp(objectPath, attributeName, objectId,
                                    registry);
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
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public Date[] getDateArrayAttribute(String objectPath, String attributeName)
    {
        final long[] timeStampArray = getTimeStampArrayAttribute(objectPath, attributeName);
        return timeStampsToDates(timeStampArray);
    }

    @Override
    public boolean isTimeStamp(String objectPath, String attributeName) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull =
                baseReader.tryGetTypeVariant(objectPath, attributeName);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeStamp();
    }

    @Override
    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = baseReader.tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeStamp();
    }

    @Override
    public HDF5TimeDuration getTimeDurationAttribute(final String objectPath,
            final String attributeName)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDuration> getAttributeRunnable =
                new ICallableWithCleanUp<HDF5TimeDuration>()
                    {
                        @Override
                        public HDF5TimeDuration call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final HDF5TimeUnit unit =
                                    baseReader.checkIsTimeDuration(objectPath, attributeName,
                                            objectId, registry);
                            final long[] data =
                                    baseReader.h5.readAttributeAsLongArray(attributeId,
                                            H5T_NATIVE_INT64, 1);
                            return new HDF5TimeDuration(data[0], unit);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public boolean isTimeDuration(String objectPath, String attributeName) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull =
                baseReader.tryGetTypeVariant(objectPath, attributeName);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeDuration();
    }

    @Override
    public HDF5TimeUnit tryGetTimeUnit(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull =
                baseReader.tryGetTypeVariant(objectPath, attributeName);
        return (typeVariantOrNull != null) ? typeVariantOrNull.tryGetTimeUnit() : null;
    }

    @Override
    public HDF5TimeDurationArray getTimeDurationArrayAttribute(final String objectPath,
            final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDurationArray> getAttributeRunnable =
                new ICallableWithCleanUp<HDF5TimeDurationArray>()
                    {
                        @Override
                        public HDF5TimeDurationArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, attributeName,
                                            objectId, registry);
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
                            return new HDF5TimeDurationArray(data, storedUnit);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = baseReader.tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeDuration();
    }

    @Override
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = baseReader.tryGetTypeVariant(objectPath);
        return (typeVariantOrNull != null) ? typeVariantOrNull.tryGetTimeUnit() : null;
    }

    @Override
    public long readTimeStamp(final String objectPath) throws HDF5JavaException
    {
        baseReader.checkOpen();
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                @Override
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    baseReader.checkIsTimeStamp(objectPath, dataSetId, registry);
                    final long[] data = new long[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readTimeStampArray(final String objectPath) throws HDF5JavaException
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
                    baseReader.checkIsTimeStamp(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readTimeStampArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
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
                    baseReader.checkIsTimeStamp(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readTimeStampArrayBlockWithOffset(final String objectPath, final int blockSize,
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
                    baseReader.checkIsTimeStamp(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public Iterable<HDF5DataBlock<long[]>> getTimeStampArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
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
                                        readTimeStampArrayBlockWithOffset(dataSetPath,
                                                index.getBlockSize(), offset);
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
    public Date readDate(final String objectPath) throws HDF5JavaException
    {
        return new Date(readTimeStamp(objectPath));
    }

    @Override
    public Date[] readDateArray(final String objectPath) throws HDF5JavaException
    {
        final long[] timeStampArray = readTimeStampArray(objectPath);
        return timeStampsToDates(timeStampArray);
    }

    private static Date[] timeStampsToDates(final long[] timeStampArray)
    {
        assert timeStampArray != null;

        final Date[] dateArray = new Date[timeStampArray.length];
        for (int i = 0; i < dateArray.length; ++i)
        {
            dateArray[i] = new Date(timeStampArray[i]);
        }
        return dateArray;
    }

    //
    // Duration
    //

    @Override
    public HDF5TimeDuration readTimeDuration(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDuration> readCallable =
                new ICallableWithCleanUp<HDF5TimeDuration>()
                    {
                        @Override
                        public HDF5TimeDuration call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, dataSetId, registry);
                            final long[] data = new long[1];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                            return new HDF5TimeDuration(data[0], storedUnit);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long readTimeDuration(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        return timeUnit.convert(readTimeDuration(objectPath));
    }

    @Override
    public HDF5TimeDuration readTimeDurationAndUnit(final String objectPath)
            throws HDF5JavaException
    {
        return readTimeDuration(objectPath);
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArray(final String objectPath)
            throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDurationArray> readCallable =
                new ICallableWithCleanUp<HDF5TimeDurationArray>()
                    {
                        @Override
                        public HDF5TimeDurationArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, dataSetId, registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final long[] data = new long[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new HDF5TimeDurationArray(data, storedUnit);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readTimeDurationArray(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        return timeUnit.convert(readTimeDurationArray(objectPath));
    }

    @Override
    public HDF5TimeDuration[] readTimeDurationAndUnitArray(final String objectPath)
            throws HDF5JavaException
    {
        final HDF5TimeDurationArray durations = readTimeDurationArray(objectPath);
        return convertTimeDurations(durations.timeUnit, durations.timeDurations);
    }

    @Override
    public long[] readTimeDurationArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final HDF5TimeUnit timeUnit)
    {
        return timeUnit.convert(readTimeDurationArrayBlock(objectPath, blockSize, blockNumber));
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber)
    {
        return readTimeDurationArrayBlockWithOffset(objectPath, blockSize, blockNumber * blockSize);
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDurationArray> readCallable =
                new ICallableWithCleanUp<HDF5TimeDurationArray>()
                    {
                        @Override
                        public HDF5TimeDurationArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, dataSetId, registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset, blockSize,
                                            registry);
                            final long[] data = new long[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new HDF5TimeDurationArray(data, storedUnit);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    @Override
    public long[] readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit)
    {
        return timeUnit
                .convert(readTimeDurationArrayBlockWithOffset(objectPath, blockSize, offset));
    }

    @Override
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException
    {
        return readTimeDurationAndUnitArrayBlockWithOffset(objectPath, blockSize, blockSize
                * blockNumber);
    }

    @Override
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException
    {
        final HDF5TimeDurationArray durations =
                readTimeDurationArrayBlockWithOffset(objectPath, blockSize, offset);
        return convertTimeDurations(durations.timeUnit, durations.timeDurations);
    }

    @Override
    public Iterable<HDF5DataBlock<HDF5TimeDuration[]>> getTimeDurationAndUnitArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException
    {
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(objectPath));

        return new Iterable<HDF5DataBlock<HDF5TimeDuration[]>>()
            {
                @Override
                public Iterator<HDF5DataBlock<HDF5TimeDuration[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<HDF5TimeDuration[]>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5DataBlock<HDF5TimeDuration[]> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final HDF5TimeDuration[] block =
                                        readTimeDurationAndUnitArrayBlockWithOffset(objectPath,
                                                index.getBlockSize(), offset);
                                return new HDF5DataBlock<HDF5TimeDuration[]>(block,
                                        index.getAndIncIndex(), offset);
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
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getTimeDurationArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException
    {
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(objectPath));

        return new Iterable<HDF5DataBlock<HDF5TimeDurationArray>>()
            {
                @Override
                public Iterator<HDF5DataBlock<HDF5TimeDurationArray>> iterator()
                {
                    return new Iterator<HDF5DataBlock<HDF5TimeDurationArray>>()
                        {
                            final HDF5NaturalBlock1DParameters.HDF5NaturalBlock1DIndex index =
                                    params.getNaturalBlockIndex();

                            @Override
                            public boolean hasNext()
                            {
                                return index.hasNext();
                            }

                            @Override
                            public HDF5DataBlock<HDF5TimeDurationArray> next()
                            {
                                final long offset = index.computeOffsetAndSizeGetOffset();
                                final HDF5TimeDurationArray block =
                                        readTimeDurationArrayBlockWithOffset(objectPath,
                                                index.getBlockSize(), offset);
                                return new HDF5DataBlock<HDF5TimeDurationArray>(block,
                                        index.getAndIncIndex(), offset);
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
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(
            final String objectPath, final HDF5TimeUnit timeUnit) throws HDF5JavaException
    {
        final HDF5NaturalBlock1DParameters params =
                new HDF5NaturalBlock1DParameters(baseReader.getDataSetInformation(objectPath));

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
                                        readTimeDurationArrayBlockWithOffset(objectPath,
                                                index.getBlockSize(), offset, timeUnit);
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

    static void convertTimeDurations(final HDF5TimeUnit toTimeUnit,
            final HDF5TimeUnit fromTimeUnit, final long[] data)
    {
        if (toTimeUnit != fromTimeUnit)
        {
            for (int i = 0; i < data.length; ++i)
            {
                data[i] = toTimeUnit.convert(data[i], fromTimeUnit);
            }
        }
    }

    static HDF5TimeDuration[] convertTimeDurations(final HDF5TimeUnit timeUnit, final long[] data)
    {
        final HDF5TimeDuration[] durations = new HDF5TimeDuration[data.length];
        for (int i = 0; i < data.length; ++i)
        {
            durations[i] = new HDF5TimeDuration(data[i], timeUnit);
        }
        return durations;
    }

}
