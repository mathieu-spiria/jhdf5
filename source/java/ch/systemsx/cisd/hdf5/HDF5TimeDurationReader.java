/*
 * Copyright 2013 ETH Zuerich, CISD
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

import java.util.Iterator;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * Implementation of {@Link IHDF5TimeDurationReader}.
 * 
 * @author Bernd Rinn
 */
class HDF5TimeDurationReader implements IHDF5TimeDurationReader
{

    private final HDF5BaseReader baseReader;

    private final HDF5LongReader longReader;

    HDF5TimeDurationReader(HDF5BaseReader baseReader, HDF5LongReader longReader)
    {
        assert baseReader != null;
        assert longReader != null;

        this.baseReader = baseReader;
        this.longReader = longReader;
    }

    @Override
    public HDF5TimeDuration getAttr(final String objectPath, final String attributeName)
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
    public HDF5TimeDurationArray getArrayAttr(final String objectPath, final String attributeName)
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
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, attributeName,
                                            objectId, registry);
                            final long[] data =
                                    longReader.getLongArrayAttribute(objectPath, attributeName);
                            return new HDF5TimeDurationArray(data, storedUnit);
                        }
                    };
        return baseReader.runner.call(getAttributeRunnable);
    }

    @Override
    public HDF5TimeDurationMDArray getMDArrayAttr(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5TimeDurationMDArray> getAttributeRunnable =
                new ICallableWithCleanUp<HDF5TimeDurationMDArray>()
                    {
                        @Override
                        public HDF5TimeDurationMDArray call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5TimeUnit storedUnit =
                                    baseReader.checkIsTimeDuration(objectPath, attributeName,
                                            objectId, registry);
                            final MDLongArray data =
                                    longReader.getLongMDArrayAttribute(objectPath, attributeName);
                            return new HDF5TimeDurationMDArray(data, storedUnit);
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
    public HDF5TimeDuration read(final String objectPath) throws HDF5JavaException
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

    public long readTimeDuration(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        return timeUnit.convert(read(objectPath));
    }

    public HDF5TimeDuration readTimeDurationAndUnit(final String objectPath)
            throws HDF5JavaException
    {
        return read(objectPath);
    }

    @Override
    public HDF5TimeDurationArray readArray(final String objectPath) throws HDF5JavaException
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

    public long[] readTimeDurationArray(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        return timeUnit.convert(readArray(objectPath));
    }

    public HDF5TimeDuration[] readTimeDurationAndUnitArray(final String objectPath)
            throws HDF5JavaException
    {
        final HDF5TimeDurationArray durations = readArray(objectPath);
        return convertTimeDurations(durations.timeUnit, durations.timeDurations);
    }

    public long[] readTimeDurationArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final HDF5TimeUnit timeUnit)
    {
        return timeUnit.convert(readArrayBlock(objectPath, blockSize, blockNumber));
    }

    @Override
    public HDF5TimeDurationArray readArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        return readArrayBlockWithOffset(objectPath, blockSize, blockNumber * blockSize);
    }

    @Override
    public HDF5TimeDurationArray readArrayBlockWithOffset(final String objectPath,
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

    public long[] readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit)
    {
        return timeUnit.convert(readArrayBlockWithOffset(objectPath, blockSize, offset));
    }

    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException
    {
        return readTimeDurationAndUnitArrayBlockWithOffset(objectPath, blockSize, blockSize
                * blockNumber);
    }

    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException
    {
        final HDF5TimeDurationArray durations =
                readArrayBlockWithOffset(objectPath, blockSize, offset);
        return convertTimeDurations(durations.timeUnit, durations.timeDurations);
    }

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
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getArrayNaturalBlocks(
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
                                        readArrayBlockWithOffset(objectPath, index.getBlockSize(),
                                                offset);
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
