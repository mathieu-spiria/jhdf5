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
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_int;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_long;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_short;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
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

    private void setTypeVariant(final int objectId, final HDF5DataTypeVariant typeVariant,
            ICleanUpRegistry registry)
    {
        baseWriter.setAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()), registry);
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
                            setStringAttribute(objectId, name, value, maxLength, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private void setStringAttribute(final int objectId, final String name, final String value,
            final int maxLength, ICleanUpRegistry registry)
    {
        final int stringDataTypeId = baseWriter.h5.createDataTypeString(maxLength + 1, registry);
        final int attributeId;
        if (baseWriter.h5.existsAttribute(objectId, name))
        {
            attributeId = baseWriter.h5.openAttribute(objectId, name, registry);
        } else
        {
            attributeId = baseWriter.h5.createAttribute(objectId, name, stringDataTypeId, registry);
        }
        baseWriter.h5.writeAttribute(attributeId, stringDataTypeId, (value + '\0').getBytes());
    }

    public void setBooleanAttribute(final String objectPath, final String name, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, new byte[]
                    { (byte) (value ? 1 : 0) });
    }

    public void setEnumAttribute(final String objectPath, final String name,
            final HDF5EnumerationValue value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        value.getType().check(baseWriter.fileId);
        final int storageDataTypeId = value.getType().getStorageTypeId();
        final int nativeDataTypeId = value.getType().getNativeTypeId();
        baseWriter.setAttribute(objectPath, name, storageDataTypeId, nativeDataTypeId, value
                .toStorageForm());
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    //
    // Generic
    //
    
    public void setDataSetSize(final String objectPath, final long newSize)
    {
        setDataSetDimensions(objectPath, new long[] { newSize });
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

    public void writeBitFieldCompact(final String objectPath, final BitSet data)
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
                                { realLength }, HDF5GenericStorageFeatures.GENERIC_COMPACT, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_B64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            BitSetConversionUtils.toStorageForm(data));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeBitField(final String objectPath, final BitSet data)
    {
        writeBitField(objectPath, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeBitField(final String objectPath, final BitSet data,
            final HDF5GenericStorageFeatures compression)
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
                                { realLength }, compression, registry);
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

    public void writeOpaqueByteArrayCompact(final String objectPath, final String tag,
            final byte[] data)
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
                                { data.length }, HDF5GenericStorageFeatures.GENERIC_COMPACT, registry);
                    H5Dwrite(dataSetId, dataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data)
    {
        writeOpaqueByteArray(objectPath, tag, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data,
            final HDF5GenericStorageFeatures compression)
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
                                { data.length }, compression, registry);
                    H5Dwrite(dataSetId, dataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, int blockSize)
    {
        return createOpaqueByteArray(objectPath, tag, 0, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize)
    {
        return createOpaqueByteArray(objectPath, tag, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize, final HDF5GenericStorageFeatures compression)
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
                    baseWriter.createDataSet(objectPath, dataTypeId, compression, new long[]
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
                    setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeScalarRunnable);
    }

    public void createTimeStampArrayCompact(final String objectPath, final long length)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE,
                                    HDF5GenericStorageFeatures.GENERIC_COMPACT, new long[]
                                        { length }, null, registry);
                    setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArrayCompact(final String objectPath, final long[] timeStamps)
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
                                { timeStamps.length }, HDF5GenericStorageFeatures.GENERIC_COMPACT,
                                    registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeStamps);
                    setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeStampArray(String objectPath, int blockSize)
    {
        createTimeStampArray(objectPath, 0, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeStampArray(final String objectPath, final long size, final int blockSize)
    {
        createTimeStampArray(objectPath, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeStampArray(final String objectPath, final long length,
            final int blockSize, final HDF5GenericStorageFeatures compression)
    {
        assert objectPath != null;
        assert length >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, compression,
                                    new long[]
                                        { length }, new long[]
                                        { blockSize }, registry);
                    setTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps)
    {
        writeTimeStampArray(objectPath, timeStamps, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps,
            final HDF5GenericStorageFeatures compression)
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
                                { timeStamps.length }, compression, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeStamps);
                    setTypeVariant(dataSetId,
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

    public void writeDateArrayCompact(final String objectPath, final Date[] dates)
    {
        writeTimeStampArrayCompact(objectPath, datesToTimeStamps(dates));
    }

    public void writeDateArray(final String objectPath, final Date[] dates)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates));
    }

    public void writeDateArray(final String objectPath, final Date[] dates,
            final HDF5GenericStorageFeatures compression)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates), compression);
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
                    setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeScalarRunnable);
    }

    public void createTimeDurationArrayCompact(final String objectPath, final long length,
            final HDF5TimeUnit timeUnit)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE,
                                    HDF5GenericStorageFeatures.GENERIC_COMPACT, new long[]
                                        { length }, null, registry);
                    setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArrayCompact(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit)
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
                                { timeDurations.length }, HDF5GenericStorageFeatures.GENERIC_COMPACT,
                                    registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeDurations);
                    setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeDurationArray(String objectPath, int blockSize, HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, 0, blockSize, timeUnit,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, size, blockSize, timeUnit,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit,
            final HDF5GenericStorageFeatures compression)
    {
        assert objectPath != null;
        assert size >= 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, compression,
                                    new long[]
                                        { size }, new long[]
                                        { blockSize }, registry);
                    setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
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
            final HDF5TimeUnit timeUnit, final HDF5IntStorageFeatures compression)
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
                                { timeDurations.length }, compression, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeDurations);
                    setTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
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

    public void writeStringCompact(final String objectPath, final String data)
    {
        writeStringCompact(objectPath, data, data.length());
    }

    public void writeStringCompact(final String objectPath, final String data, final int maxLength)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    baseWriter.writeScalarString(objectPath, data, maxLength);
                    return null; // Nothing to return.
                }

            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeString(final String objectPath, final String data, final int maxLength)
    {
        writeString(objectPath, data, maxLength, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data)
    {
        writeString(objectPath, data, data.length(), HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data,
            final HDF5GenericStorageFeatures compression)
    {
        writeString(objectPath, data, data.length(), compression);
    }

    // Implementation note: this needs special treatment as we want to create a (possibly chunked)
    // data set with max dimension 1 instead of infinity.
    public void writeString(final String objectPath, final String data, final int maxLength,
            final HDF5GenericStorageFeatures compression)
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
                            HDF5Utils.tryGetChunkSizeForString(realMaxLength, compression
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
                                        stringDataTypeId, compression, objectPath, layout,
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
            final HDF5GenericStorageFeatures compression)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data), compression);
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
        writeStringArray(objectPath, data, maxLength, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
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
            final HDF5GenericStorageFeatures compression) throws HDF5JavaException
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
                                    realMaxLength, false, compression, registry);
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                            maxLength);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    private int getDataSetIdForArray(final String objectPath, final int dataTypeId,
            final int arrayLength, final int elementLength,
            final boolean enforceCompactLayoutOnCreate, final HDF5GenericStorageFeatures compression,
            ICleanUpRegistry registry)
    {
        int dataSetId;
        final long[] dimensions = new long[]
            { arrayLength };
        if (exists(objectPath, false))
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
                    HDF5Utils.tryGetChunkSizeForStringVector(arrayLength, elementLength,
                            compression.requiresChunking(), baseWriter.useExtentableDataTypes);
            final HDF5StorageLayout layout =
                    baseWriter.determineLayout(dataTypeId, dimensions, chunkSizeOrNull,
                            enforceCompactLayoutOnCreate ? HDF5StorageLayout.COMPACT : null);
            dataSetId =
                    baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, chunkSizeOrNull,
                            dataTypeId, compression, objectPath, layout, baseWriter.fileFormat,
                            registry);
        }
        return dataSetId;
    }

    public void createStringArray(final String objectPath, final int maxLength, final int blockSize)
    {
        createStringArray(objectPath, maxLength, 0, blockSize, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize)
    {
        createStringArray(objectPath, maxLength, size, blockSize, GENERIC_NO_COMPRESSION);
    }

    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures compression) throws HDF5JavaException
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
                    final int dataSetId;
                    final long[] dimensions = new long[]
                        { size };
                    if (exists(objectPath, false))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                        final HDF5StorageLayout layout =
                                baseWriter.h5.getLayout(dataSetId, registry);
                        if (layout == HDF5StorageLayout.CHUNKED)
                        {
                            // Safety check. JHDF5 creates CHUNKED data sets always with unlimited
                            // max dimensions but we may have to work on a file we haven't created.
                            if (baseWriter.areDimensionsInBounds(dataSetId, dimensions))
                            {
                                baseWriter.h5.setDataSetExtentChunked(dataSetId, dimensions);
                            } else
                            {
                                throw new HDF5JavaException(
                                        "New data set dimension is out of bounds.");
                            }
                        } else
                        {
                            // CONTIGUOUS and COMPACT data sets are effectively fixed size, thus we
                            // need to delete and re-create it.
                            baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
                            baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, null,
                                    stringDataTypeId,
                                    HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION, objectPath,
                                    layout, baseWriter.fileFormat, registry);
                        }
                    } else
                    {
                        dataSetId =
                                baseWriter.h5.createDataSet(baseWriter.fileId, dimensions,
                                        new long[]
                                            { blockSize }, stringDataTypeId, compression,
                                        objectPath, HDF5StorageLayout.CHUNKED,
                                        baseWriter.fileFormat, registry);
                    }
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
                                    pointerSize, false, GENERIC_NO_COMPRESSION, registry);
                    baseWriter.writeStringVL(dataSetId, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createStringVariableLengthArray(final String objectPath, final int blockSize)
    {
        createStringVariableLengthArray(objectPath, 0, blockSize);
    }

    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize) throws HDF5JavaException
    {
        assert objectPath != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int stringDataTypeId = baseWriter.variableLengthStringDataTypeId;
                    final int dataSetId;
                    final long[] dimensions = new long[]
                        { size };
                    if (exists(objectPath, false))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                        final HDF5StorageLayout layout =
                                baseWriter.h5.getLayout(dataSetId, registry);
                        if (layout == HDF5StorageLayout.CHUNKED)
                        {
                            // Safety check. JHDF5 creates CHUNKED data sets always with unlimited
                            // max dimensions but we may have to work on a file we haven't created.
                            if (baseWriter.areDimensionsInBounds(dataSetId, dimensions))
                            {
                                baseWriter.h5.setDataSetExtentChunked(dataSetId, dimensions);
                            } else
                            {
                                throw new HDF5JavaException(
                                        "New data set dimension is out of bounds.");
                            }
                        } else
                        {
                            // CONTIGUOUS and COMPACT data sets are effectively fixed size, thus we
                            // need to delete and re-create it.
                            baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
                            baseWriter.h5.createDataSet(baseWriter.fileId, dimensions, null,
                                    stringDataTypeId, GENERIC_NO_COMPRESSION, objectPath, layout,
                                    baseWriter.fileFormat, registry);
                        }
                    } else
                    {
                        dataSetId =
                                baseWriter.h5.createDataSet(baseWriter.fileId, dimensions,
                                        new long[]
                                            { blockSize }, stringDataTypeId,
                                        GENERIC_NO_COMPRESSION, objectPath,
                                        HDF5StorageLayout.CHUNKED, baseWriter.fileFormat, registry);
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
        return getEnumType(name, values, true);
    }

    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values,
            final boolean check) throws HDF5JavaException
    {
        baseWriter.checkOpen();
        final String dataTypePath = HDF5Utils.createDataTypePath(HDF5Utils.ENUM_PREFIX, name);
        int storageDataTypeId = baseWriter.getDataTypeId(dataTypePath);
        if (storageDataTypeId < 0)
        {
            storageDataTypeId = baseWriter.h5.createDataTypeEnum(values, baseWriter.fileRegistry);
            baseWriter.commitDataType(dataTypePath, storageDataTypeId);
        } else if (check)
        {
            checkEnumValues(storageDataTypeId, values, name);
        }
        final int nativeDataTypeId =
                baseWriter.h5.getNativeDataType(storageDataTypeId, baseWriter.fileRegistry);
        return new HDF5EnumerationType(baseWriter.fileId, storageDataTypeId, nativeDataTypeId,
                name, values);
    }

    public void writeEnum(final String objectPath, final HDF5EnumerationValue value)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert value != null;

        baseWriter.checkOpen();
        value.getType().check(baseWriter.fileId);
        final int storageDataTypeId = value.getType().getStorageTypeId();
        final int nativeDataTypeId = value.getType().getNativeTypeId();
        baseWriter.writeScalar(objectPath, storageDataTypeId, nativeDataTypeId, value
                .toStorageForm());
    }

    public void writeEnumArrayCompact(final String objectPath, final HDF5EnumerationValueArray data)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        data.getType().check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, data.getType().getStorageTypeId(),
                                    new long[]
                                        { data.getLength() },
                                    HDF5GenericStorageFeatures.GENERIC_COMPACT, registry);
                    switch (data.getStorageForm())
                    {
                        case BYTE:
                            H5Dwrite(dataSetId, data.getType().getNativeTypeId(), H5S_ALL, H5S_ALL,
                                    H5P_DEFAULT, data.getStorageFormBArray());
                            break;
                        case SHORT:
                            H5Dwrite_short(dataSetId, data.getType().getNativeTypeId(), H5S_ALL,
                                    H5S_ALL, H5P_DEFAULT, data.getStorageFormSArray());
                            break;
                        case INT:
                            H5Dwrite_int(dataSetId, data.getType().getNativeTypeId(), H5S_ALL,
                                    H5S_ALL, H5P_DEFAULT, data.getStorageFormIArray());
                            break;
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeEnumArray(final String objectPath, final HDF5EnumerationValueArray data)
            throws HDF5JavaException
    {
        writeEnumArray(objectPath, data, HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void writeEnumArray(final String objectPath, final HDF5EnumerationValueArray data,
            final HDF5IntStorageFeatures compression) throws HDF5JavaException
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        data.getType().check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    if (compression.isScaling())
                    {
                        compression.checkScalingOK(baseWriter.fileFormat);
                        final HDF5IntStorageFeatures actualCompression =
                                HDF5IntStorageFeatures.createDeflateAndIntegerScaling(compression
                                        .getDeflateLevel(), data.getType().getNumberOfBits());
                        final int dataSetId =
                                baseWriter.getDataSetId(objectPath, data.getType()
                                        .getIntStorageTypeId(), new long[]
                                    { data.getLength() }, actualCompression, registry);
                        switch (data.getStorageForm())
                        {
                            case BYTE:
                                H5Dwrite(dataSetId, H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                                        data.getStorageFormBArray());
                                break;
                            case SHORT:
                                H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL,
                                        H5P_DEFAULT, data.getStorageFormSArray());
                                break;
                            case INT:
                                H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL,
                                        H5P_DEFAULT, data.getStorageFormIArray());
                                break;
                        }
                        setTypeVariant(dataSetId, HDF5DataTypeVariant.ENUM, registry);
                        setStringAttribute(dataSetId, HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE, data
                                .getType().getName(), data.getType().getName().length(), registry);
                    } else
                    {
                        final int dataSetId =
                                baseWriter.getDataSetId(objectPath, data.getType()
                                        .getStorageTypeId(), new long[]
                                    { data.getLength() }, compression, registry);
                        switch (data.getStorageForm())
                        {
                            case BYTE:
                                H5Dwrite(dataSetId, data.getType().getNativeTypeId(), H5S_ALL,
                                        H5S_ALL, H5P_DEFAULT, data.getStorageFormBArray());
                                break;
                            case SHORT:
                                H5Dwrite_short(dataSetId, data.getType().getNativeTypeId(),
                                        H5S_ALL, H5S_ALL, H5P_DEFAULT, data.getStorageFormSArray());
                                break;
                            case INT:
                                H5Dwrite_int(dataSetId, data.getType().getNativeTypeId(), H5S_ALL,
                                        H5S_ALL, H5P_DEFAULT, data.getStorageFormIArray());
                        }
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createEnumArray(final String objectPath, final HDF5EnumerationType enumType,
            final int blockSize)
    {
        createEnumArray(objectPath, enumType, 0, blockSize, HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void createEnumArray(final String objectPath, final HDF5EnumerationType enumType,
            final long size, final int blockSize)
    {
        createEnumArray(objectPath, enumType, size, blockSize,
                HDF5IntStorageFeatures.INT_NO_COMPRESSION);
    }

    public void createEnumArray(final String objectPath, final HDF5EnumerationType enumType,
            final long size, final int blockSize, final HDF5IntStorageFeatures compression)
    {
        baseWriter.checkOpen();
        enumType.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    if (compression.isScaling())
                    {
                        compression.checkScalingOK(baseWriter.fileFormat);
                        final HDF5IntStorageFeatures actualCompression =
                                HDF5IntStorageFeatures.createDeflateAndIntegerScaling(compression
                                        .getDeflateLevel(), enumType.getNumberOfBits());
                        final int dataSetId =
                                baseWriter.createDataSet(objectPath,
                                        enumType.getIntStorageTypeId(), actualCompression,
                                        new long[]
                                            { size }, new long[]
                                            { blockSize }, registry);
                        setTypeVariant(dataSetId, HDF5DataTypeVariant.ENUM, registry);
                        setStringAttribute(dataSetId, HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE, enumType
                                .getName(), enumType.getName().length(), registry);
                    } else
                    {
                        baseWriter.createDataSet(objectPath, enumType.getStorageTypeId(),
                                compression, new long[]
                                    { size }, new long[]
                                    { blockSize }, registry);
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createRunnable);
    }

    public void writeEnumArrayBlock(final String objectPath, final HDF5EnumerationValueArray data,
            final long blockNumber)
    {
        assert objectPath != null;
        assert data != null;

        writeEnumArrayBlockWithOffset(objectPath, data, data.getLength(), data.getLength()
                * blockNumber);
    }

    public void writeEnumArrayBlockWithOffset(final String objectPath,
            final HDF5EnumerationValueArray data, final int dataSize, final long offset)
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
                    if (isScaledEnum(dataSetId, registry))
                    {
                        switch (data.getStorageForm())
                        {
                            case BYTE:
                                H5Dwrite(dataSetId, H5T_NATIVE_INT8, memorySpaceId, dataSpaceId,
                                        H5P_DEFAULT, data.getStorageFormBArray());
                                break;
                            case SHORT:
                                H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, memorySpaceId,
                                        dataSpaceId, H5P_DEFAULT, data.getStorageFormSArray());
                                break;
                            case INT:
                                H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, memorySpaceId,
                                        dataSpaceId, H5P_DEFAULT, data.getStorageFormIArray());
                                break;
                        }
                        setTypeVariant(dataSetId, HDF5DataTypeVariant.ENUM, registry);
                        setStringAttribute(dataSetId, HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE, data
                                .getType().getName(), data.getType().getName().length(), registry);
                    } else
                    {
                        switch (data.getStorageForm())
                        {
                            case BYTE:
                                H5Dwrite(dataSetId, data.getType().getNativeTypeId(),
                                        memorySpaceId, dataSpaceId, H5P_DEFAULT, data
                                                .getStorageFormBArray());
                                break;
                            case SHORT:
                                H5Dwrite_short(dataSetId, data.getType().getNativeTypeId(),
                                        memorySpaceId, dataSpaceId, H5P_DEFAULT, data
                                                .getStorageFormSArray());
                                break;
                            case INT:
                                H5Dwrite_int(dataSetId, data.getType().getNativeTypeId(),
                                        memorySpaceId, dataSpaceId, H5P_DEFAULT, data
                                                .getStorageFormIArray());
                        }
                    }
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    //
    // Compound
    //

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> compoundType,
            HDF5CompoundMemberMapping... members)
    {
        baseWriter.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectByteifyer = createByteifyers(compoundType, members);
        final String dataTypeName = (name != null) ? name : compoundType.getSimpleName();
        final int storageDataTypeId =
                getOrCreateCompoundDataType(dataTypeName, compoundType, objectByteifyer);
        final int nativeDataTypeId = createNativeCompoundDataType(objectByteifyer);
        return new HDF5CompoundType<T>(baseWriter.fileId, storageDataTypeId, nativeDataTypeId,
                dataTypeName, compoundType, objectByteifyer);
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> compoundType,
            HDF5CompoundMemberMapping... members)
    {
        return getCompoundType(null, compoundType, members);
    }

    private <T> int getOrCreateCompoundDataType(final String dataTypeName,
            final Class<T> compoundClass, final HDF5ValueObjectByteifyer<T> objectByteifyer)
    {
        final String dataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX, dataTypeName);
        int storageDataTypeId = baseWriter.getDataTypeId(dataTypePath);
        if (storageDataTypeId < 0)
        {
            storageDataTypeId = createStorageCompoundDataType(objectByteifyer);
            baseWriter.commitDataType(dataTypePath, storageDataTypeId);
        }
        return storageDataTypeId;
    }

    public <T> void writeCompound(final String objectPath, final HDF5CompoundType<T> type,
            final T data)
    {
        writeCompound(objectPath, type, data, null);
    }

    public <T> void writeCompound(final String objectPath, final HDF5CompoundType<T> type,
            final T data, final IByteArrayInspector inspectorOrNull)
    {
        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final byte[] byteArray = type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
        if (inspectorOrNull != null)
        {
            inspectorOrNull.inspect(byteArray);
        }
        baseWriter.writeScalar(objectPath, type.getStorageTypeId(), type.getNativeTypeId(),
                byteArray);
    }

    public <T> void writeCompoundArrayCompact(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data)
    {
        writeCompoundArray(objectPath, type, data, HDF5GenericStorageFeatures.GENERIC_COMPACT);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data)
    {
        writeCompoundArray(objectPath, type, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures compression)
    {
        writeCompoundArray(objectPath, type, data, compression, null);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericStorageFeatures compression,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, type.getStorageTypeId(), new long[]
                                { data.length }, compression, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber)
    {
        writeCompoundArrayBlock(objectPath, type, data, blockNumber, null);
    }

    public <T> void writeCompoundArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert blockNumber >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long size = data.length;
                    final long[] dimensions = new long[]
                        { size };
                    final long[] offset = new long[]
                        { size * blockNumber };
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { data.length * (blockNumber + 1) }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset)
    {
        writeCompoundArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final long size = data.length;
        final long[] dimensions = new long[]
            { size };
        final long[] offsetArray = new long[]
            { offset };
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, new long[]
                                        { offset + data.length }, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offsetArray, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(), data);
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, int blockSize)
    {
        createCompoundArray(objectPath, type, 0, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize)
    {
        createCompoundArray(objectPath, type, size, blockSize,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize, final HDF5GenericStorageFeatures compression)
    {
        assert objectPath != null;
        assert type != null;
        assert size >= 0;
        assert blockSize >= 0 && (blockSize <= size || size == 0);

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, type.getStorageTypeId(), compression,
                            new long[]
                                { size }, new long[]
                                { blockSize }, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data)
    {
        writeCompoundMDArray(objectPath, type, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures compression)
    {
        writeCompoundMDArray(objectPath, type, data, compression, null);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericStorageFeatures compression,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, type.getStorageTypeId(), MDArray
                                    .toLong(data.dimensions()), compression, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockDimensions)
    {
        writeCompoundMDArrayBlock(objectPath, type, data, blockDimensions, null);
    }

    public <T> void writeCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockDimensions,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final long[] dimensions = data.longDimensions();
        final long[] offset = new long[dimensions.length];
        final long[] dataSetDimensions = new long[dimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockDimensions[i] * dimensions[i];
            dataSetDimensions[i] = offset[i] + dimensions[i];
        }
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset)
    {
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data, offset, null);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset,
            final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long[] dimensions = data.longDimensions();
                    final long[] dataSetDimensions = new long[dimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + dimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, dimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(dimensions, registry);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset)
    {
        writeCompoundMDArrayBlockWithOffset(objectPath, type, data, blockDimensions, offset,
                memoryOffset, null);
    }

    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset, final IByteArrayInspector inspectorOrNull)
    {
        assert objectPath != null;
        assert type != null;
        assert data != null;
        assert offset != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final long[] memoryDimensions = data.longDimensions();
                    final long[] longBlockDimensions = MDArray.toLong(blockDimensions);
                    final long[] dataSetDimensions = new long[blockDimensions.length];
                    for (int i = 0; i < offset.length; ++i)
                    {
                        dataSetDimensions[i] = offset[i] + blockDimensions[i];
                    }
                    final int dataSetId =
                            baseWriter.h5.openAndExtendDataSet(baseWriter.fileId, objectPath,
                                    baseWriter.fileFormat, dataSetDimensions, -1, registry);
                    final int dataSpaceId =
                            baseWriter.h5.getDataSpaceForDataSet(dataSetId, registry);
                    baseWriter.h5.setHyperslabBlock(dataSpaceId, offset, longBlockDimensions);
                    final int memorySpaceId =
                            baseWriter.h5.createSimpleDataSpace(memoryDimensions, registry);
                    baseWriter.h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset),
                            longBlockDimensions);
                    final byte[] byteArray =
                            type.getObjectByteifyer().byteify(type.getStorageTypeId(),
                                    data.getAsFlatArray());
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArray);
                    }
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] blockDimensions)
    {
        createCompoundMDArray(objectPath, type, new long[blockDimensions.length], blockDimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions)
    {
        createCompoundMDArray(objectPath, type, dimensions, blockDimensions,
                HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions,
            final HDF5GenericStorageFeatures compression)
    {
        assert objectPath != null;
        assert type != null;
        assert dimensions != null;
        assert blockDimensions != null;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, type.getStorageTypeId(), compression,
                            dimensions, MDArray.toLong(blockDimensions), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
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
