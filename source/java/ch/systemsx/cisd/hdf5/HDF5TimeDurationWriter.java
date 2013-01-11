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

import static ch.systemsx.cisd.hdf5.hdf5lib.H5D.H5Dwrite;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5S_ALL;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_STD_I64LE;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.hdf5lib.HDFNativeData;

/**
 * Implementation of {@link IHDF5TimeDurationWriter}.
 * 
 * @author Bernd Rinn
 */
public class HDF5TimeDurationWriter extends HDF5TimeDurationReader implements
        IHDF5TimeDurationWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5TimeDurationWriter(HDF5BaseWriter baseWriter, HDF5LongReader longReader)
    {
        super(baseWriter, longReader);

        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    @Override
    public void setAttr(String objectPath, String name, long timeDuration, HDF5TimeUnit timeUnit)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, timeUnit.getTypeVariant(), H5T_STD_I64LE,
                H5T_NATIVE_INT64, new long[]
                    { timeDuration });
    }

    @Override
    public void setAttr(String objectPath, String name, HDF5TimeDuration timeDuration)
    {
        setAttr(objectPath, name, timeDuration.getValue(), timeDuration.getUnit());
    }

    @Override
    public void setArrayAttr(final String objectPath, final String name,
            final HDF5TimeDurationArray timeDurations)
    {
        assert objectPath != null;
        assert name != null;
        assert timeDurations != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> setAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT64,
                                    timeDurations.timeDurations.length, registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I64LE,
                                    timeDurations.timeDurations.length, registry);
                    baseWriter.setAttribute(objectPath, name,
                            timeDurations.timeUnit.getTypeVariant(), storageTypeId, memoryTypeId,
                            timeDurations.timeDurations);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    @Override
    public void setMDArrayAttr(final String objectPath, final String attributeName,
            final HDF5TimeDurationMDArray timeDurations)
    {
        assert objectPath != null;
        assert attributeName != null;
        assert timeDurations != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> setAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    final int memoryTypeId =
                            baseWriter.h5.createArrayType(H5T_NATIVE_INT64,
                                    timeDurations.timeDurations.dimensions(), registry);
                    final int storageTypeId =
                            baseWriter.h5.createArrayType(H5T_STD_I64LE,
                                    timeDurations.timeDurations.dimensions(), registry);
                    baseWriter.setAttribute(objectPath, attributeName,
                            timeDurations.timeUnit.getTypeVariant(), storageTypeId, memoryTypeId,
                            timeDurations.timeDurations.getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(setAttributeRunnable);
    }

    public void writeTimeDuration(final String objectPath, final long timeDuration)
    {
        write(objectPath, timeDuration, HDF5TimeUnit.SECONDS);
    }

    @Override
    public void write(final String objectPath, final long timeDuration, final HDF5TimeUnit timeUnit)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeScalarRunnable = new ICallableWithCleanUp<Object>()
            {
                @Override
                public Object call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.writeScalar(objectPath, H5T_STD_I64LE, H5T_NATIVE_INT64,
                                    HDFNativeData.longToByte(timeDuration), true, true, registry);
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeScalarRunnable);
    }

    @Override
    public void write(String objectPath, HDF5TimeDuration timeDuration)
    {
        write(objectPath, timeDuration.getValue(), timeDuration.getUnit());
    }

    @Override
    public void createArray(String objectPath, int size, HDF5TimeUnit timeUnit)
    {
        createArray(objectPath, size, timeUnit, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    @Override
    public void createArray(final String objectPath, final long size, final int blockSize,
            final HDF5TimeUnit timeUnit)
    {
        createArray(objectPath, size, blockSize, timeUnit,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    @Override
    public void createArray(final String objectPath, final int size, final HDF5TimeUnit timeUnit,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    final int longBytes = 8;
                    final int dataSetId;
                    if (features.requiresChunking())
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { 0 }, new long[]
                                            { size }, longBytes, registry);
                    } else
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { size }, null, longBytes, registry);
                    }
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    @Override
    public void createArray(final String objectPath, final long size, final int blockSize,
            final HDF5TimeUnit timeUnit, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    final int longBytes = 8;
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                    new long[]
                                        { size }, new long[]
                                        { blockSize }, longBytes, registry);
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations)
    {
        writeTimeDurationArray(objectPath, timeDurations, HDF5TimeUnit.SECONDS,
                HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    @Override
    public void writeArray(String objectPath, HDF5TimeDurationArray timeDurations)
    {
        writeArray(objectPath, timeDurations, HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    @Override
    public void writeArray(final String objectPath, final HDF5TimeDurationArray timeDurations,
            final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert timeDurations != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    final int longBytes = 8;
                    final int dataSetId =
                            baseWriter.getOrCreateDataSetId(objectPath, H5T_STD_I64LE, new long[]
                                { timeDurations.timeDurations.length }, longBytes, features,
                                    registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeDurations.timeDurations);
                    baseWriter.setTypeVariant(dataSetId, timeDurations.timeUnit.getTypeVariant(),
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit)
    {
        writeTimeDurationArray(objectPath, timeDurations, timeUnit,
                HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit, final HDF5IntStorageFeatures features)
    {
        writeArray(objectPath, new HDF5TimeDurationArray(timeDurations, timeUnit));
    }

    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations)
    {
        writeTimeDurationArray(objectPath, timeDurations, HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations, final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert timeDurations != null;

        if (timeDurations.length == 0)
        {
            return;
        }
        final HDF5TimeDurationArray durations = HDF5TimeDurationArray.create(timeDurations);
        writeArray(objectPath, durations);
    }

    @Override
    public void writeArrayBlock(String objectPath, HDF5TimeDurationArray data, long blockNumber)
    {
        writeArrayBlockWithOffset(objectPath, data, data.getLength(), data.getLength()
                * blockNumber);
    }

    @Override
    public void writeArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDurationArray data, final int dataSize, final long offset)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
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
                    final HDF5TimeUnit storedUnit =
                            baseWriter.checkIsTimeDuration(objectPath, dataSetId, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite(dataSetId, H5T_NATIVE_INT64, memorySpaceId, dataSpaceId, H5P_DEFAULT,
                            storedUnit.convert(data));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArrayBlock(final String objectPath, final long[] data,
            final long blockNumber, final HDF5TimeUnit timeUnit)
    {
        writeTimeDurationArrayBlockWithOffset(objectPath, data, data.length, data.length
                * blockNumber, timeUnit);
    }

    public void writeTimeDurationArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset, final HDF5TimeUnit timeUnit)
    {
        writeArrayBlockWithOffset(objectPath, new HDF5TimeDurationArray(data, timeUnit), dataSize,
                offset);
    }

    public void writeTimeDurationArrayBlock(final String objectPath, final HDF5TimeDuration[] data,
            final long blockNumber)
    {
        writeTimeDurationArrayBlockWithOffset(objectPath, data, data.length, data.length
                * blockNumber);
    }

    public void writeTimeDurationArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDuration[] data, final int dataSize, final long offset)
    {
        writeArrayBlockWithOffset(objectPath, HDF5TimeDurationArray.create(data), dataSize, offset);
    }

}
