/*
 * Copyright 2007 ETH Zuerich, CISD.
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

import static ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION;
import static ch.systemsx.cisd.hdf5.HDF5Utils.OPAQUE_PREFIX;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_ATTRIBUTE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.createDataTypePath;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_long;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_B64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE;

import java.util.BitSet;
import java.util.Date;

import ncsa.hdf.hdf5lib.HDFNativeData;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A class for writing HDF5 files (HDF5 1.6.x or HDF5 1.8.x).
 * <p>
 * The class focuses on ease of use instead of completeness. As a consequence not all valid HDF5
 * files can be generated using this class, but only a subset.
 * <p>
 * Usage:
 * 
 * <pre>
 * float[] f = new float[100];
 * ...
 * HDF5Writer writer = new HDF5WriterConfig(&quot;test.h5&quot;).writer();
 * writer.writeFloatArray(&quot;/some/path/dataset&quot;, f);
 * writer.addAttribute(&quot;some key&quot;, &quot;some value&quot;);
 * writer.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
final class HDF5Writer extends HDF5Reader implements IHDF5Writer
{
    private final HDF5BaseWriter baseWriter;

    private final IHDF5ByteWriter byteWriter;

    private final IHDF5ShortWriter shortWriter;

    private final IHDF5IntWriter intWriter;

    private final IHDF5LongWriter longWriter;

    private final IHDF5FloatWriter floatWriter;

    private final IHDF5DoubleWriter doubleWriter;

    private final IHDF5EnumWriter enumWriter;

    private final IHDF5CompoundWriter compoundWriter;

    HDF5Writer(HDF5BaseWriter baseWriter)
    {
        super(baseWriter);
        this.baseWriter = baseWriter;
        this.byteWriter = new HDF5ByteWriter(baseWriter);
        this.shortWriter = new HDF5ShortWriter(baseWriter);
        this.intWriter = new HDF5IntWriter(baseWriter);
        this.longWriter = new HDF5LongWriter(baseWriter);
        this.floatWriter = new HDF5FloatWriter(baseWriter);
        this.doubleWriter = new HDF5DoubleWriter(baseWriter);
        this.enumWriter = new HDF5EnumWriter(baseWriter);
        this.compoundWriter = new HDF5CompoundWriter(baseWriter);
    }

    HDF5BaseWriter getBaseWriter()
    {
        return baseWriter;
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    public boolean isUseExtendableDataTypes()
    {
        return baseWriter.useExtentableDataTypes;
    }

    public FileFormat getFileFormat()
    {
        return baseWriter.fileFormat;
    }

    // /////////////////////
    // File
    // /////////////////////

    public void flush()
    {
        baseWriter.checkOpen();
        baseWriter.flush();
    }

    public void flushSyncBlocking()
    {
        baseWriter.checkOpen();
        baseWriter.flushSyncBlocking();
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    public void createHardLink(String currentPath, String newPath)
    {
        assert currentPath != null;
        assert newPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createHardLink(baseWriter.fileId, currentPath, newPath);
    }

    public void createSoftLink(String targetPath, String linkPath)
    {
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createSoftLink(baseWriter.fileId, linkPath, targetPath);
    }

    public void createOrUpdateSoftLink(String targetPath, String linkPath)
    {
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        if (isSymbolicLink(linkPath))
        {
            delete(linkPath);
        }
        baseWriter.h5.createSoftLink(baseWriter.fileId, linkPath, targetPath);
    }

    public void createExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException
    {
        assert targetFileName != null;
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        if (baseWriter.fileFormat.isHDF5_1_8_OK() == false)
        {
            throw new IllegalStateException(
                    "External links are not allowed in strict HDF5 1.6.x compatibility mode.");
        }
        baseWriter.h5.createExternalLink(baseWriter.fileId, linkPath, targetFileName, targetPath);
    }

    public void createOrUpdateExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException
    {
        assert targetFileName != null;
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        if (baseWriter.fileFormat.isHDF5_1_8_OK() == false)
        {
            throw new IllegalStateException(
                    "External links are not allowed in strict HDF5 1.6.x compatibility mode.");
        }
        if (isSymbolicLink(linkPath))
        {
            delete(linkPath);
        }
        baseWriter.h5.createExternalLink(baseWriter.fileId, linkPath, targetFileName, targetPath);
    }

    public void delete(String objectPath)
    {
        baseWriter.checkOpen();
        if (isGroup(objectPath, false))
        {
            for (String path : getGroupMemberPaths(objectPath))
            {
                delete(path);
            }
        }
        baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
    }

    public void move(String oldLinkPath, String newLinkPath)
    {
        baseWriter.checkOpen();
        baseWriter.h5.moveLink(baseWriter.fileId, oldLinkPath, newLinkPath);
    }

    // /////////////////////
    // Group
    // /////////////////////

    public void createGroup(final String groupPath)
    {
        baseWriter.checkOpen();
        baseWriter.h5.createGroup(baseWriter.fileId, groupPath);
    }

    public void createGroup(final String groupPath, final int sizeHint)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createGroupRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.h5.createOldStyleGroup(baseWriter.fileId, groupPath, sizeHint,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createGroupRunnable);
    }

    public void createGroup(final String groupPath, final int maxCompact, final int minDense)
    {
        baseWriter.checkOpen();
        if (baseWriter.fileFormat.isHDF5_1_8_OK() == false)
        {
            throw new IllegalStateException(
                    "New style groups are not allowed in strict HDF5 1.6.x compatibility mode.");
        }
        final ICallableWithCleanUp<Void> createGroupRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.h5.createNewStyleGroup(baseWriter.fileId, groupPath, maxCompact,
                            minDense, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createGroupRunnable);
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void deleteAttribute(final String objectPath, final String name)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> deleteAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseWriter.h5.openObject(baseWriter.fileId, objectPath, registry);
                    baseWriter.h5.deleteAttribute(objectId, name);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(deleteAttributeRunnable);
    }

    public void setTypeVariant(final String objectPath, final HDF5DataTypeVariant typeVariant)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()));
    }

    public void deleteTypeVariant(String objectPath)
    {
        deleteAttribute(objectPath, TYPE_VARIANT_ATTRIBUTE);
    }

    public void setStringAttributeVariableLength(final String objectPath, final String name,
            final String value)
    {
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            setStringAttributeVariableLength(objectId, name, value, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private void setStringAttributeVariableLength(final int objectId, final String name,
            final String value, ICleanUpRegistry registry)
    {
        final int attributeId;
        if (baseWriter.h5.existsAttribute(objectId, name))
        {
            attributeId = baseWriter.h5.openAttribute(objectId, name, registry);
        } else
        {
            attributeId =
                    baseWriter.h5.createAttribute(objectId, name,
                            baseWriter.variableLengthStringDataTypeId, registry);
        }
        baseWriter.writeAttributeStringVL(attributeId, new String[]
            { value });
    }

    public void setStringAttribute(final String objectPath, final String name, final String value)
    {
        setStringAttribute(objectPath, name, value, value.length());
    }

    public void setStringAttribute(final String objectPath, final String name, final String value,
            final int maxLength)
    {
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            baseWriter.setStringAttribute(objectId, name, value, maxLength,
                                    registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    public void setBooleanAttribute(final String objectPath, final String name, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, new byte[]
                    { (byte) (value ? 1 : 0) });
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    //
    // Generic
    //

    public void setDataSetSize(final String objectPath, final long newSize)
    {
        setDataSetDimensions(objectPath, new long[]
            { newSize });
    }

    public void setDataSetDimensions(final String objectPath, final long[] newDimensions)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.setDataSetDimensions(objectPath, newDimensions, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    //
    // Boolean
    //

    public void writeBoolean(final String objectPath, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, HDFNativeData.byteToByte((byte) (value ? 1 : 0)));
    }

    public void writeBitField(final String objectPath, final BitSet data)
    {
        writeBitField(objectPath, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeBitField(final String objectPath, final BitSet data,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int msb = data.length();
                    final int realLength = msb / 64 + (msb % 64 != 0 ? 1 : 0);
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_B64LE, new long[]
                                { realLength }, features, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_B64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            BitSetConversionUtils.toStorageForm(data));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    //
    // Opaque
    //

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data)
    {
        writeOpaqueByteArray(objectPath, tag, data,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert tag != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataTypeId = getOrCreateOpaqueTypeId(tag);
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, dataTypeId, new long[]
                                { data.length }, features, registry);
                    H5Dwrite(dataSetId, dataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, int size)
    {
        return createOpaqueByteArray(objectPath, tag, size,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize)
    {
        return createOpaqueByteArray(objectPath, tag, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final int size, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert tag != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final int dataTypeId = getOrCreateOpaqueTypeId(tag);
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, dataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, dataTypeId, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
        return new HDF5OpaqueType(baseWriter.fileId, dataTypeId, tag);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert tag != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        final int dataTypeId = getOrCreateOpaqueTypeId(tag);
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, dataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
        return new HDF5OpaqueType(baseWriter.fileId, dataTypeId, tag);
    }

    public void writeOpaqueByteArrayBlock(final String objectPath, final HDF5OpaqueType dataType,
            final byte[] data, final long blockNumber)
    {
        assert objectPath != null;
        assert dataType != null;
        assert data != null;

        baseWriter.checkOpen();
        dataType.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final long[] blockDimensions = new long[]
                        { data.length };
                    final long[] slabStartOrNull = new long[]
                        { data.length * blockNumber };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { data.length * (blockNumber + 1) }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite(dataSetId, dataType.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeOpaqueByteArrayBlockWithOffset(final String objectPath,
            final HDF5OpaqueType dataType, final byte[] data, final int dataSize, final long offset)
    {
        assert objectPath != null;
        assert dataType != null;
        assert data != null;

        baseWriter.checkOpen();
        dataType.check(baseWriter.fileId);
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
                    H5Dwrite(dataSetId, dataType.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    private int getOrCreateOpaqueTypeId(final String tag)
    {
        final String dataTypePath = createDataTypePath(OPAQUE_PREFIX, tag);
        int dataTypeId = baseWriter.getDataTypeId(dataTypePath);
        if (dataTypeId < 0)
        {
            dataTypeId = baseWriter.h5.createDataTypeOpaque(1, tag, baseWriter.fileRegistry);
            baseWriter.commitDataType(dataTypePath, dataTypeId);
        }
        return dataTypeId;
    }

    //
    // Date
    //

    public void writeTimeStamp(final String objectPath, final long timeStamp)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeScalarRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.writeScalar(objectPath, H5T_STD_I64LE, H5T_NATIVE_INT64,
                                    HDFNativeData.longToByte(timeStamp), registry);
                    baseWriter.setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeScalarRunnable);
    }

    public void createTimeStampArray(String objectPath, int size)
    {
        createTimeStampArray(objectPath, size, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeStampArray(final String objectPath, final long size, final int blockSize)
    {
        createTimeStampArray(objectPath, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeStampArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId;
                    if (features.requiresChunking())
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { 0 }, new long[]
                                            { size }, registry);
                    } else
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { size }, null, registry);
                    }
                    baseWriter.setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeStampArray(final String objectPath, final long length,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert length >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                    new long[]
                                        { length }, new long[]
                                        { blockSize }, registry);
                    baseWriter.setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps)
    {
        writeTimeStampArray(objectPath, timeStamps,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert timeStamps != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I64LE, new long[]
                                { timeStamps.length }, features, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeStamps);
                    baseWriter.setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArrayBlock(final String objectPath, final long[] data,
            final long blockNumber)
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
                    final long[] slabStartOrNull = new long[]
                        { data.length * blockNumber };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { data.length * (blockNumber + 1) }, -1, registry);
                    checkIsTimeStamp(objectPath, dataSetId, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArrayBlockWithOffset(final String objectPath, final long[] data,
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
                    checkIsTimeStamp(objectPath, dataSetId, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeDate(final String objectPath, final Date date)
    {
        writeTimeStamp(objectPath, date.getTime());
    }

    public void writeDateArray(final String objectPath, final Date[] dates)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates));
    }

    public void writeDateArray(final String objectPath, final Date[] dates,
            final HDF5GenericStorageFeatures features)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates), features);
    }

    private static long[] datesToTimeStamps(Date[] dates)
    {
        assert dates != null;

        final long[] timestamps = new long[dates.length];
        for (int i = 0; i < timestamps.length; ++i)
        {
            timestamps[i] = dates[i].getTime();
        }
        return timestamps;
    }

    //
    // Duration
    //

    public void writeTimeDuration(final String objectPath, final long timeDuration)
    {
        writeTimeDuration(objectPath, timeDuration, HDF5TimeUnit.SECONDS);
    }

    public void writeTimeDuration(final String objectPath, final long timeDuration,
            final HDF5TimeUnit timeUnit)
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeScalarRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.writeScalar(objectPath, H5T_STD_I64LE, H5T_NATIVE_INT64,
                                    HDFNativeData.longToByte(timeDuration), registry);
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeScalarRunnable);
    }

    public void createTimeDurationArray(String objectPath, int size, HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, size, timeUnit,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, size, blockSize, timeUnit,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeDurationArray(final String objectPath, final int size,
            final HDF5TimeUnit timeUnit, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId;
                    if (features.requiresChunking())
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { 0 }, new long[]
                                            { size }, registry);
                    } else
                    {
                        dataSetId =
                                baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                        new long[]
                                            { size }, null, registry);
                    }
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, features,
                                    new long[]
                                        { size }, new long[]
                                        { blockSize }, registry);
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

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit)
    {
        writeTimeDurationArray(objectPath, timeDurations, timeUnit,
                HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit, final HDF5IntStorageFeatures features)
    {
        assert objectPath != null;
        assert timeDurations != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_I64LE, new long[]
                                { timeDurations.length }, features, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeDurations);
                    baseWriter.setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArrayBlock(final String objectPath, final long[] data,
            final long blockNumber, final HDF5TimeUnit timeUnit)
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
                    final long[] slabStartOrNull = new long[]
                        { data.length * blockNumber };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { data.length * (blockNumber + 1) }, -1, registry);
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    convertTimeDurations(timeUnit, storedUnit, data);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset, final HDF5TimeUnit timeUnit)
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
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, slabStartOrNull, blockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(blockDimensions, registry);
                    convertTimeDurations(timeUnit, storedUnit, data);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    //
    // String
    //

    public void writeString(final String objectPath, final String data, final int maxLength)
    {
        writeString(objectPath, data, maxLength, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data)
    {
        writeString(objectPath, data, data.length(),
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data,
            final HDF5GenericStorageFeatures features)
    {
        writeString(objectPath, data, data.length(), features);
    }

    // Implementation note: this needs special treatment as we want to create a (possibly chunked)
    // data set with max dimension 1 instead of infinity.
    public void writeString(final String objectPath, final String data, final int maxLength,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int realMaxLength = maxLength + 1; // trailing '\0'
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(realMaxLength, registry);
                    final long[] chunkSizeOrNull =
                            HDF5Utils.tryGetChunkSizeForString(realMaxLength, features
                                    .requiresChunking());
                    final int dataSetId;
                    if (exists(objectPath, false))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    } else
                    {
                        final HDF5StorageLayout layout =
                                baseWriter.determineLayout(stringDataTypeId,
                                        HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull, null);
                        dataSetId =
                                baseWriter.h5.createDataSet(baseWriter.fileId,
                                        HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull,
                                        stringDataTypeId, features, objectPath, layout,
                                        baseWriter.fileFormat, registry);
                    }
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            (data + '\0').getBytes());
                    return null; // Nothing to return.
                }

            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringArray(final String objectPath, final String[] data,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data), features);
    }

    public void writeStringArray(final String objectPath, final String[] data)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data),
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeStringArray(final String objectPath, final String[] data, final int maxLength)
    {
        writeStringArray(objectPath, data, maxLength,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    private static int getMaxLength(String[] data)
    {
        int maxLength = 0;
        for (String s : data)
        {
            maxLength = Math.max(maxLength, s.length());
        }
        return maxLength;
    }

    public void writeStringArray(final String objectPath, final String[] data, final int maxLength,
            final HDF5GenericStorageFeatures features) throws HDF5JavaException
    {
        assert objectPath != null;
        assert data != null;
        assert maxLength >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int realMaxLength = maxLength + 1; // Trailing '\0'
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(realMaxLength, registry);
                    final int dataSetId =
                            getDataSetIdForArray(objectPath, stringDataTypeId, data.length,
                                    realMaxLength, features, registry);
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                            maxLength);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    private int getDataSetIdForArray(final String objectPath, final int dataTypeId,
            final int arrayLength, final int elementLength,
            final HDF5GenericStorageFeatures features, ICleanUpRegistry registry)
    {
        int dataSetId;
        final long[] dimensions = new long[]
            { arrayLength };
        boolean exists = exists(objectPath, false);
        if (exists && features.isKeepDataSetIfExists() == false)
        {
            baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
            exists = false;
        }
        if (exists)
        {
            dataSetId = baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
            final HDF5StorageLayout layout = baseWriter.h5.getLayout(dataSetId, registry);
            if (layout == HDF5StorageLayout.CHUNKED)
            {
                // Safety check. JHDF5 creates CHUNKED data sets always with unlimited
                // max dimensions but we may have to work on a file we haven't created.
                if (baseWriter.areDimensionsInBounds(dataSetId, dimensions))
                {
                    baseWriter.h5.setDataSetExtentChunked(dataSetId, dimensions);
                } else
                {
                    throw new HDF5JavaException("New data set dimension is out of bounds.");
                }
            } else
            {
                // CONTIGUOUS and COMPACT data sets are fixed size, thus we need to
                // delete and re-create it.
                baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
                dataSetId =
                        baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, null,
                                dataTypeId, GENERIC_NO_COMPRESSION, objectPath, layout,
                                baseWriter.fileFormat, registry);
            }
        } else
        {
            final long[] chunkSizeOrNull =
                    HDF5Utils.tryGetChunkSizeForStringVector(arrayLength, elementLength, features
                            .requiresChunking(), baseWriter.useExtentableDataTypes);
            final HDF5StorageLayout layout =
                    baseWriter.determineLayout(dataTypeId, dimensions, chunkSizeOrNull, features
                            .tryGetProposedLayout());
            dataSetId =
                    baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, chunkSizeOrNull,
                            dataTypeId, features, objectPath, layout, baseWriter.fileFormat,
                            registry);
        }
        return dataSetId;
    }

    public void createStringArray(final String objectPath, final int maxLength, final int size)
    {
        createStringArray(objectPath, maxLength, size, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize)
    {
        createStringArray(objectPath, maxLength, size, blockSize, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final int size,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert maxLength > 0;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(maxLength + 1, registry);
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert maxLength > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(maxLength + 1, registry);
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringArrayBlock(final String objectPath, final String[] data,
            final long blockNumber)
    {
        assert data != null;
        writeStringArrayBlockWithOffset(objectPath, data, data.length, data.length * blockNumber);
    }

    public void writeStringArrayBlockWithOffset(final String objectPath, final String[] data,
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
                    final int stringDataTypeId =
                            baseWriter.h5.getDataTypeForDataSet(dataSetId, registry);
                    if (baseWriter.h5.isVariableLengthString(stringDataTypeId))
                    {
                        baseWriter.writeStringVL(dataSetId, memorySpaceId, dataSpaceId, data);
                    } else
                    {
                        final int maxLength = baseWriter.h5.getDataTypeSize(stringDataTypeId) - 1;
                        H5Dwrite(dataSetId, stringDataTypeId, memorySpaceId, dataSpaceId,
                                H5P_DEFAULT, data, maxLength);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLength(final String objectPath, final String data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    baseWriter.writeScalarString(objectPath, data, -1);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeStringVariableLengthArray(final String objectPath, final String[] data)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int pointerSize = 8; // 64bit pointers
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    final int dataSetId =
                            getDataSetIdForArray(objectPath, stringDataTypeId, data.length,
                                    pointerSize, GENERIC_NO_COMPRESSION, registry);
                    baseWriter.writeStringVL(dataSetId, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringVariableLengthArray(final String objectPath, final int size)
    {
        createStringVariableLengthArray(objectPath, size,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize) throws HDF5JavaException
    {
        createStringVariableLengthArray(objectPath, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;
        assert blockSize > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                        { size }, new long[]
                        { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringVariableLengthArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    if (features.requiresChunking())
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { 0 }, new long[]
                            { size }, registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, stringDataTypeId, features, new long[]
                            { size }, null, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    //
    // Enum
    //

    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return enumWriter.getEnumType(name, values);
    }

    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values,
            final boolean check) throws HDF5JavaException
    {
        return enumWriter.getEnumType(name, values, check);
    }

    public void createEnumArray(String objectPath, HDF5EnumerationType enumType, int size)
    {
        enumWriter.createEnumArray(objectPath, enumType, size);
    }

    public void createEnumArray(String objectPath, HDF5EnumerationType enumType, long size,
            HDF5IntStorageFeatures features)
    {
        enumWriter.createEnumArray(objectPath, enumType, size, features);
    }

    public void createEnumArray(String objectPath, HDF5EnumerationType enumType, long size,
            int blockSize, HDF5IntStorageFeatures features)
    {
        enumWriter.createEnumArray(objectPath, enumType, size, blockSize, features);
    }

    public void createEnumArray(String objectPath, HDF5EnumerationType enumType, long size,
            int blockSize)
    {
        enumWriter.createEnumArray(objectPath, enumType, size, blockSize);
    }

    public void setEnumAttribute(String objectPath, String name, HDF5EnumerationValue value)
    {
        enumWriter.setEnumAttribute(objectPath, name, value);
    }

    public void writeEnum(String objectPath, HDF5EnumerationValue value) throws HDF5JavaException
    {
        enumWriter.writeEnum(objectPath, value);
    }

    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data,
            HDF5IntStorageFeatures features) throws HDF5JavaException
    {
        enumWriter.writeEnumArray(objectPath, data, features);
    }

    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data)
            throws HDF5JavaException
    {
        enumWriter.writeEnumArray(objectPath, data);
    }

    public void writeEnumArrayBlock(String objectPath, HDF5EnumerationValueArray data,
            long blockNumber)
    {
        enumWriter.writeEnumArrayBlock(objectPath, data, blockNumber);
    }

    public void writeEnumArrayBlockWithOffset(String objectPath, HDF5EnumerationValueArray data,
            int dataSize, long offset)
    {
        enumWriter.writeEnumArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    //
    // Compound
    //

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> compoundType,
            HDF5CompoundMemberMapping... members)
    {
        return compoundWriter.getCompoundType(name, compoundType, members);
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> compoundType,
            HDF5CompoundMemberMapping... members)
    {
        return compoundWriter.getCompoundType(compoundType, members);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, int size)
    {
        compoundWriter.createCompoundArray(objectPath, type, size);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.createCompoundArray(objectPath, type, size, features);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            int blockSize, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createCompoundArray(objectPath, type, size, blockSize, features);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            int blockSize)
    {
        compoundWriter.createCompoundArray(objectPath, type, size, blockSize);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createCompoundMDArray(objectPath, type, dimensions, features);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions)
    {
        compoundWriter.createCompoundMDArray(objectPath, type, dimensions);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            long[] dimensions, int[] blockDimensions, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createCompoundMDArray(objectPath, type, dimensions, blockDimensions,
                features);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            long[] dimensions, int[] blockDimensions)
    {
        compoundWriter.createCompoundMDArray(objectPath, type, dimensions, blockDimensions);
    }

    public <T> void writeCompound(String objectPath, HDF5CompoundType<T> type, T data,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompound(objectPath, type, data, inspectorOrNull);
    }

    public <T> void writeCompound(String objectPath, HDF5CompoundType<T> type, T data)
    {
        compoundWriter.writeCompound(objectPath, type, data);
    }

    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data,
            HDF5GenericStorageFeatures features, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundArray(objectPath, type, data, features, inspectorOrNull);
    }

    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeCompoundArray(objectPath, type, data, features);
    }

    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data)
    {
        compoundWriter.writeCompoundArray(objectPath, type, data);
    }

    public <T> void writeCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type, T[] data,
            long blockNumber, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter
                .writeCompoundArrayBlock(objectPath, type, data, blockNumber, inspectorOrNull);
    }

    public <T> void writeCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type, T[] data,
            long blockNumber)
    {
        compoundWriter.writeCompoundArrayBlock(objectPath, type, data, blockNumber);
    }

    public <T> void writeCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            T[] data, long offset, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundArrayBlockWithOffset(objectPath, type, data, offset,
                inspectorOrNull);
    }

    public <T> void writeCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            T[] data, long offset)
    {
        compoundWriter.writeCompoundArrayBlockWithOffset(objectPath, type, data, offset);
    }

    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, HDF5GenericStorageFeatures features,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundMDArray(objectPath, type, data, features, inspectorOrNull);
    }

    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeCompoundMDArray(objectPath, type, data, features);
    }

    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data)
    {
        compoundWriter.writeCompoundMDArray(objectPath, type, data);
    }

    public <T> void writeCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, long[] blockDimensions, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundMDArrayBlock(objectPath, type, data, blockDimensions,
                inspectorOrNull);
    }

    public <T> void writeCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, long[] blockDimensions)
    {
        compoundWriter.writeCompoundMDArrayBlock(objectPath, type, data, blockDimensions);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, int[] blockDimensions, long[] offset,
            int[] memoryOffset, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundMDArrayBlockWithOffset(objectPath, type, data, blockDimensions,
                offset, memoryOffset, inspectorOrNull);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, int[] blockDimensions, long[] offset,
            int[] memoryOffset)
    {
        compoundWriter.writeCompoundMDArrayBlockWithOffset(objectPath, type, data, blockDimensions,
                offset, memoryOffset);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, long[] offset,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeCompoundMDArrayBlockWithOffset(objectPath, type, data, offset,
                inspectorOrNull);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, long[] offset)
    {
        compoundWriter.writeCompoundMDArrayBlockWithOffset(objectPath, type, data, offset);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - START
    // ------------------------------------------------------------------------------

    public void createByteArray(String objectPath, int blockSize)
    {
        byteWriter.createByteArray(objectPath, blockSize);
    }

    public void createByteArray(String objectPath, long size, int blockSize)
    {
        byteWriter.createByteArray(objectPath, size, blockSize);
    }

    public void createByteArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        byteWriter.createByteArray(objectPath, size, features);
    }

    public void createByteArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createByteArray(objectPath, size, blockSize, features);
    }

    public void createByteMDArray(String objectPath, int[] blockDimensions)
    {
        byteWriter.createByteMDArray(objectPath, blockDimensions);
    }

    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        byteWriter.createByteMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createByteMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createByteMDArray(objectPath, dimensions, features);
    }

    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createByteMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createByteMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        byteWriter.createByteMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        byteWriter.createByteMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        byteWriter.createByteMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setByteArrayAttribute(String objectPath, String name, byte[] value)
    {
        byteWriter.setByteArrayAttribute(objectPath, name, value);
    }

    public void setByteAttribute(String objectPath, String name, byte value)
    {
        byteWriter.setByteAttribute(objectPath, name, value);
    }

    public void setByteMDArrayAttribute(String objectPath, String name, MDByteArray value)
    {
        byteWriter.setByteMDArrayAttribute(objectPath, name, value);
    }

    public void setByteMatrixAttribute(String objectPath, String name, byte[][] value)
    {
        byteWriter.setByteMatrixAttribute(objectPath, name, value);
    }

    public void writeByte(String objectPath, byte value)
    {
        byteWriter.writeByte(objectPath, value);
    }

    public void writeByteArray(String objectPath, byte[] data)
    {
        byteWriter.writeByteArray(objectPath, data);
    }

    public void writeByteArray(String objectPath, byte[] data, HDF5IntStorageFeatures features)
    {
        byteWriter.writeByteArray(objectPath, data, features);
    }

    public void writeByteArrayBlock(String objectPath, byte[] data, long blockNumber)
    {
        byteWriter.writeByteArrayBlock(objectPath, data, blockNumber);
    }

    public void writeByteArrayBlockWithOffset(String objectPath, byte[] data, int dataSize,
            long offset)
    {
        byteWriter.writeByteArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeByteMDArray(String objectPath, MDByteArray data)
    {
        byteWriter.writeByteMDArray(objectPath, data);
    }

    public void writeByteMDArray(String objectPath, MDByteArray data,
            HDF5IntStorageFeatures features)
    {
        byteWriter.writeByteMDArray(objectPath, data, features);
    }

    public void writeByteMDArrayBlock(String objectPath, MDByteArray data, long[] blockNumber)
    {
        byteWriter.writeByteMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeByteMDArrayBlockWithOffset(String objectPath, MDByteArray data, long[] offset)
    {
        byteWriter.writeByteMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeByteMDArrayBlockWithOffset(String objectPath, MDByteArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        byteWriter.writeByteMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeByteMatrix(String objectPath, byte[][] data)
    {
        byteWriter.writeByteMatrix(objectPath, data);
    }

    public void writeByteMatrix(String objectPath, byte[][] data, HDF5IntStorageFeatures features)
    {
        byteWriter.writeByteMatrix(objectPath, data, features);
    }

    public void writeByteMatrixBlock(String objectPath, byte[][] data, long blockNumberX,
            long blockNumberY)
    {
        byteWriter.writeByteMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeByteMatrixBlockWithOffset(String objectPath, byte[][] data, long offsetX,
            long offsetY)
    {
        byteWriter.writeByteMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeByteMatrixBlockWithOffset(String objectPath, byte[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        byteWriter.writeByteMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    public void createDoubleArray(String objectPath, int blockSize)
    {
        doubleWriter.createDoubleArray(objectPath, blockSize);
    }

    public void createDoubleArray(String objectPath, long size, int blockSize)
    {
        doubleWriter.createDoubleArray(objectPath, size, blockSize);
    }

    public void createDoubleArray(String objectPath, int size, HDF5FloatStorageFeatures features)
    {
        doubleWriter.createDoubleArray(objectPath, size, features);
    }

    public void createDoubleArray(String objectPath, long size, int blockSize,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createDoubleArray(objectPath, size, blockSize, features);
    }

    public void createDoubleMDArray(String objectPath, int[] blockDimensions)
    {
        doubleWriter.createDoubleMDArray(objectPath, blockDimensions);
    }

    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        doubleWriter.createDoubleMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createDoubleMDArray(String objectPath, int[] dimensions,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createDoubleMDArray(objectPath, dimensions, features);
    }

    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createDoubleMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createDoubleMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        doubleWriter.createDoubleMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        doubleWriter.createDoubleMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatStorageFeatures features)
    {
        doubleWriter.createDoubleMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setDoubleArrayAttribute(String objectPath, String name, double[] value)
    {
        doubleWriter.setDoubleArrayAttribute(objectPath, name, value);
    }

    public void setDoubleAttribute(String objectPath, String name, double value)
    {
        doubleWriter.setDoubleAttribute(objectPath, name, value);
    }

    public void setDoubleMDArrayAttribute(String objectPath, String name, MDDoubleArray value)
    {
        doubleWriter.setDoubleMDArrayAttribute(objectPath, name, value);
    }

    public void setDoubleMatrixAttribute(String objectPath, String name, double[][] value)
    {
        doubleWriter.setDoubleMatrixAttribute(objectPath, name, value);
    }

    public void writeDouble(String objectPath, double value)
    {
        doubleWriter.writeDouble(objectPath, value);
    }

    public void writeDoubleArray(String objectPath, double[] data)
    {
        doubleWriter.writeDoubleArray(objectPath, data);
    }

    public void writeDoubleArray(String objectPath, double[] data, HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeDoubleArray(objectPath, data, features);
    }

    public void writeDoubleArrayBlock(String objectPath, double[] data, long blockNumber)
    {
        doubleWriter.writeDoubleArrayBlock(objectPath, data, blockNumber);
    }

    public void writeDoubleArrayBlockWithOffset(String objectPath, double[] data, int dataSize,
            long offset)
    {
        doubleWriter.writeDoubleArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeDoubleMDArray(String objectPath, MDDoubleArray data)
    {
        doubleWriter.writeDoubleMDArray(objectPath, data);
    }

    public void writeDoubleMDArray(String objectPath, MDDoubleArray data,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeDoubleMDArray(objectPath, data, features);
    }

    public void writeDoubleMDArrayBlock(String objectPath, MDDoubleArray data, long[] blockNumber)
    {
        doubleWriter.writeDoubleMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray data,
            long[] offset)
    {
        doubleWriter.writeDoubleMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        doubleWriter.writeDoubleMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeDoubleMatrix(String objectPath, double[][] data)
    {
        doubleWriter.writeDoubleMatrix(objectPath, data);
    }

    public void writeDoubleMatrix(String objectPath, double[][] data,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeDoubleMatrix(objectPath, data, features);
    }

    public void writeDoubleMatrixBlock(String objectPath, double[][] data, long blockNumberX,
            long blockNumberY)
    {
        doubleWriter.writeDoubleMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeDoubleMatrixBlockWithOffset(String objectPath, double[][] data, long offsetX,
            long offsetY)
    {
        doubleWriter.writeDoubleMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeDoubleMatrixBlockWithOffset(String objectPath, double[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        doubleWriter.writeDoubleMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    public void createFloatArray(String objectPath, int blockSize)
    {
        floatWriter.createFloatArray(objectPath, blockSize);
    }

    public void createFloatArray(String objectPath, long size, int blockSize)
    {
        floatWriter.createFloatArray(objectPath, size, blockSize);
    }

    public void createFloatArray(String objectPath, int size, HDF5FloatStorageFeatures features)
    {
        floatWriter.createFloatArray(objectPath, size, features);
    }

    public void createFloatArray(String objectPath, long size, int blockSize,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createFloatArray(objectPath, size, blockSize, features);
    }

    public void createFloatMDArray(String objectPath, int[] blockDimensions)
    {
        floatWriter.createFloatMDArray(objectPath, blockDimensions);
    }

    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        floatWriter.createFloatMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createFloatMDArray(String objectPath, int[] dimensions,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createFloatMDArray(objectPath, dimensions, features);
    }

    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createFloatMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createFloatMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        floatWriter.createFloatMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        floatWriter.createFloatMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatStorageFeatures features)
    {
        floatWriter.createFloatMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setFloatArrayAttribute(String objectPath, String name, float[] value)
    {
        floatWriter.setFloatArrayAttribute(objectPath, name, value);
    }

    public void setFloatAttribute(String objectPath, String name, float value)
    {
        floatWriter.setFloatAttribute(objectPath, name, value);
    }

    public void setFloatMDArrayAttribute(String objectPath, String name, MDFloatArray value)
    {
        floatWriter.setFloatMDArrayAttribute(objectPath, name, value);
    }

    public void setFloatMatrixAttribute(String objectPath, String name, float[][] value)
    {
        floatWriter.setFloatMatrixAttribute(objectPath, name, value);
    }

    public void writeFloat(String objectPath, float value)
    {
        floatWriter.writeFloat(objectPath, value);
    }

    public void writeFloatArray(String objectPath, float[] data)
    {
        floatWriter.writeFloatArray(objectPath, data);
    }

    public void writeFloatArray(String objectPath, float[] data, HDF5FloatStorageFeatures features)
    {
        floatWriter.writeFloatArray(objectPath, data, features);
    }

    public void writeFloatArrayBlock(String objectPath, float[] data, long blockNumber)
    {
        floatWriter.writeFloatArrayBlock(objectPath, data, blockNumber);
    }

    public void writeFloatArrayBlockWithOffset(String objectPath, float[] data, int dataSize,
            long offset)
    {
        floatWriter.writeFloatArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeFloatMDArray(String objectPath, MDFloatArray data)
    {
        floatWriter.writeFloatMDArray(objectPath, data);
    }

    public void writeFloatMDArray(String objectPath, MDFloatArray data,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.writeFloatMDArray(objectPath, data, features);
    }

    public void writeFloatMDArrayBlock(String objectPath, MDFloatArray data, long[] blockNumber)
    {
        floatWriter.writeFloatMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray data, long[] offset)
    {
        floatWriter.writeFloatMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        floatWriter.writeFloatMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeFloatMatrix(String objectPath, float[][] data)
    {
        floatWriter.writeFloatMatrix(objectPath, data);
    }

    public void writeFloatMatrix(String objectPath, float[][] data,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.writeFloatMatrix(objectPath, data, features);
    }

    public void writeFloatMatrixBlock(String objectPath, float[][] data, long blockNumberX,
            long blockNumberY)
    {
        floatWriter.writeFloatMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeFloatMatrixBlockWithOffset(String objectPath, float[][] data, long offsetX,
            long offsetY)
    {
        floatWriter.writeFloatMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeFloatMatrixBlockWithOffset(String objectPath, float[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        floatWriter.writeFloatMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    public void createIntArray(String objectPath, int blockSize)
    {
        intWriter.createIntArray(objectPath, blockSize);
    }

    public void createIntArray(String objectPath, long size, int blockSize)
    {
        intWriter.createIntArray(objectPath, size, blockSize);
    }

    public void createIntArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        intWriter.createIntArray(objectPath, size, features);
    }

    public void createIntArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        intWriter.createIntArray(objectPath, size, blockSize, features);
    }

    public void createIntMDArray(String objectPath, int[] blockDimensions)
    {
        intWriter.createIntMDArray(objectPath, blockDimensions);
    }

    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        intWriter.createIntMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createIntMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        intWriter.createIntMDArray(objectPath, dimensions, features);
    }

    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        intWriter.createIntMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createIntMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        intWriter.createIntMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        intWriter.createIntMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        intWriter.createIntMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setIntArrayAttribute(String objectPath, String name, int[] value)
    {
        intWriter.setIntArrayAttribute(objectPath, name, value);
    }

    public void setIntAttribute(String objectPath, String name, int value)
    {
        intWriter.setIntAttribute(objectPath, name, value);
    }

    public void setIntMDArrayAttribute(String objectPath, String name, MDIntArray value)
    {
        intWriter.setIntMDArrayAttribute(objectPath, name, value);
    }

    public void setIntMatrixAttribute(String objectPath, String name, int[][] value)
    {
        intWriter.setIntMatrixAttribute(objectPath, name, value);
    }

    public void writeInt(String objectPath, int value)
    {
        intWriter.writeInt(objectPath, value);
    }

    public void writeIntArray(String objectPath, int[] data)
    {
        intWriter.writeIntArray(objectPath, data);
    }

    public void writeIntArray(String objectPath, int[] data, HDF5IntStorageFeatures features)
    {
        intWriter.writeIntArray(objectPath, data, features);
    }

    public void writeIntArrayBlock(String objectPath, int[] data, long blockNumber)
    {
        intWriter.writeIntArrayBlock(objectPath, data, blockNumber);
    }

    public void writeIntArrayBlockWithOffset(String objectPath, int[] data, int dataSize,
            long offset)
    {
        intWriter.writeIntArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeIntMDArray(String objectPath, MDIntArray data)
    {
        intWriter.writeIntMDArray(objectPath, data);
    }

    public void writeIntMDArray(String objectPath, MDIntArray data, HDF5IntStorageFeatures features)
    {
        intWriter.writeIntMDArray(objectPath, data, features);
    }

    public void writeIntMDArrayBlock(String objectPath, MDIntArray data, long[] blockNumber)
    {
        intWriter.writeIntMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeIntMDArrayBlockWithOffset(String objectPath, MDIntArray data, long[] offset)
    {
        intWriter.writeIntMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeIntMDArrayBlockWithOffset(String objectPath, MDIntArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        intWriter.writeIntMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeIntMatrix(String objectPath, int[][] data)
    {
        intWriter.writeIntMatrix(objectPath, data);
    }

    public void writeIntMatrix(String objectPath, int[][] data, HDF5IntStorageFeatures features)
    {
        intWriter.writeIntMatrix(objectPath, data, features);
    }

    public void writeIntMatrixBlock(String objectPath, int[][] data, long blockNumberX,
            long blockNumberY)
    {
        intWriter.writeIntMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeIntMatrixBlockWithOffset(String objectPath, int[][] data, long offsetX,
            long offsetY)
    {
        intWriter.writeIntMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeIntMatrixBlockWithOffset(String objectPath, int[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        intWriter.writeIntMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    public void createLongArray(String objectPath, int blockSize)
    {
        longWriter.createLongArray(objectPath, blockSize);
    }

    public void createLongArray(String objectPath, long size, int blockSize)
    {
        longWriter.createLongArray(objectPath, size, blockSize);
    }

    public void createLongArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        longWriter.createLongArray(objectPath, size, features);
    }

    public void createLongArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        longWriter.createLongArray(objectPath, size, blockSize, features);
    }

    public void createLongMDArray(String objectPath, int[] blockDimensions)
    {
        longWriter.createLongMDArray(objectPath, blockDimensions);
    }

    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        longWriter.createLongMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createLongMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        longWriter.createLongMDArray(objectPath, dimensions, features);
    }

    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        longWriter.createLongMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createLongMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        longWriter.createLongMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        longWriter.createLongMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        longWriter.createLongMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setLongArrayAttribute(String objectPath, String name, long[] value)
    {
        longWriter.setLongArrayAttribute(objectPath, name, value);
    }

    public void setLongAttribute(String objectPath, String name, long value)
    {
        longWriter.setLongAttribute(objectPath, name, value);
    }

    public void setLongMDArrayAttribute(String objectPath, String name, MDLongArray value)
    {
        longWriter.setLongMDArrayAttribute(objectPath, name, value);
    }

    public void setLongMatrixAttribute(String objectPath, String name, long[][] value)
    {
        longWriter.setLongMatrixAttribute(objectPath, name, value);
    }

    public void writeLong(String objectPath, long value)
    {
        longWriter.writeLong(objectPath, value);
    }

    public void writeLongArray(String objectPath, long[] data)
    {
        longWriter.writeLongArray(objectPath, data);
    }

    public void writeLongArray(String objectPath, long[] data, HDF5IntStorageFeatures features)
    {
        longWriter.writeLongArray(objectPath, data, features);
    }

    public void writeLongArrayBlock(String objectPath, long[] data, long blockNumber)
    {
        longWriter.writeLongArrayBlock(objectPath, data, blockNumber);
    }

    public void writeLongArrayBlockWithOffset(String objectPath, long[] data, int dataSize,
            long offset)
    {
        longWriter.writeLongArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeLongMDArray(String objectPath, MDLongArray data)
    {
        longWriter.writeLongMDArray(objectPath, data);
    }

    public void writeLongMDArray(String objectPath, MDLongArray data,
            HDF5IntStorageFeatures features)
    {
        longWriter.writeLongMDArray(objectPath, data, features);
    }

    public void writeLongMDArrayBlock(String objectPath, MDLongArray data, long[] blockNumber)
    {
        longWriter.writeLongMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeLongMDArrayBlockWithOffset(String objectPath, MDLongArray data, long[] offset)
    {
        longWriter.writeLongMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeLongMDArrayBlockWithOffset(String objectPath, MDLongArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        longWriter.writeLongMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeLongMatrix(String objectPath, long[][] data)
    {
        longWriter.writeLongMatrix(objectPath, data);
    }

    public void writeLongMatrix(String objectPath, long[][] data, HDF5IntStorageFeatures features)
    {
        longWriter.writeLongMatrix(objectPath, data, features);
    }

    public void writeLongMatrixBlock(String objectPath, long[][] data, long blockNumberX,
            long blockNumberY)
    {
        longWriter.writeLongMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeLongMatrixBlockWithOffset(String objectPath, long[][] data, long offsetX,
            long offsetY)
    {
        longWriter.writeLongMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeLongMatrixBlockWithOffset(String objectPath, long[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        longWriter.writeLongMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    public void createShortArray(String objectPath, int blockSize)
    {
        shortWriter.createShortArray(objectPath, blockSize);
    }

    public void createShortArray(String objectPath, long size, int blockSize)
    {
        shortWriter.createShortArray(objectPath, size, blockSize);
    }

    public void createShortArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        shortWriter.createShortArray(objectPath, size, features);
    }

    public void createShortArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createShortArray(objectPath, size, blockSize, features);
    }

    public void createShortMDArray(String objectPath, int[] blockDimensions)
    {
        shortWriter.createShortMDArray(objectPath, blockDimensions);
    }

    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        shortWriter.createShortMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createShortMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createShortMDArray(objectPath, dimensions, features);
    }

    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createShortMDArray(objectPath, dimensions, blockDimensions, features);
    }

    public void createShortMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        shortWriter.createShortMatrix(objectPath, blockSizeX, blockSizeY);
    }

    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        shortWriter.createShortMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        shortWriter.createShortMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    public void setShortArrayAttribute(String objectPath, String name, short[] value)
    {
        shortWriter.setShortArrayAttribute(objectPath, name, value);
    }

    public void setShortAttribute(String objectPath, String name, short value)
    {
        shortWriter.setShortAttribute(objectPath, name, value);
    }

    public void setShortMDArrayAttribute(String objectPath, String name, MDShortArray value)
    {
        shortWriter.setShortMDArrayAttribute(objectPath, name, value);
    }

    public void setShortMatrixAttribute(String objectPath, String name, short[][] value)
    {
        shortWriter.setShortMatrixAttribute(objectPath, name, value);
    }

    public void writeShort(String objectPath, short value)
    {
        shortWriter.writeShort(objectPath, value);
    }

    public void writeShortArray(String objectPath, short[] data)
    {
        shortWriter.writeShortArray(objectPath, data);
    }

    public void writeShortArray(String objectPath, short[] data, HDF5IntStorageFeatures features)
    {
        shortWriter.writeShortArray(objectPath, data, features);
    }

    public void writeShortArrayBlock(String objectPath, short[] data, long blockNumber)
    {
        shortWriter.writeShortArrayBlock(objectPath, data, blockNumber);
    }

    public void writeShortArrayBlockWithOffset(String objectPath, short[] data, int dataSize,
            long offset)
    {
        shortWriter.writeShortArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    public void writeShortMDArray(String objectPath, MDShortArray data)
    {
        shortWriter.writeShortMDArray(objectPath, data);
    }

    public void writeShortMDArray(String objectPath, MDShortArray data,
            HDF5IntStorageFeatures features)
    {
        shortWriter.writeShortMDArray(objectPath, data, features);
    }

    public void writeShortMDArrayBlock(String objectPath, MDShortArray data, long[] blockNumber)
    {
        shortWriter.writeShortMDArrayBlock(objectPath, data, blockNumber);
    }

    public void writeShortMDArrayBlockWithOffset(String objectPath, MDShortArray data, long[] offset)
    {
        shortWriter.writeShortMDArrayBlockWithOffset(objectPath, data, offset);
    }

    public void writeShortMDArrayBlockWithOffset(String objectPath, MDShortArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        shortWriter.writeShortMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    public void writeShortMatrix(String objectPath, short[][] data)
    {
        shortWriter.writeShortMatrix(objectPath, data);
    }

    public void writeShortMatrix(String objectPath, short[][] data, HDF5IntStorageFeatures features)
    {
        shortWriter.writeShortMatrix(objectPath, data, features);
    }

    public void writeShortMatrixBlock(String objectPath, short[][] data, long blockNumberX,
            long blockNumberY)
    {
        shortWriter.writeShortMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    public void writeShortMatrixBlockWithOffset(String objectPath, short[][] data, long offsetX,
            long offsetY)
    {
        shortWriter.writeShortMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    public void writeShortMatrixBlockWithOffset(String objectPath, short[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        shortWriter.writeShortMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - END
    // ------------------------------------------------------------------------------
}
