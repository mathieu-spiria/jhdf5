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

import static ch.systemsx.cisd.hdf5.HDF5Utils.OPAQUE_PREFIX;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_ATTRIBUTE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.createDataTypePath;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.H5.H5DwriteString;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_int;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_long;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite_short;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_IEEE_F64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_DOUBLE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_B64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I16LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I8LE;

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
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;
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
        if (isGroup(objectPath))
        {
            for (String path : getGroupMemberPaths(objectPath))
            {
                delete(path);
            }
        }
        baseWriter.h5.deleteObject(baseWriter.fileId, objectPath);
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

    public void addEnumAttribute(final String objectPath, final String name,
            final HDF5EnumerationValue value)
    {
        assert objectPath != null;
        assert name != null;
        assert value != null;

        baseWriter.checkOpen();
        value.getType().check(baseWriter.fileId);
        final int storageDataTypeId = value.getType().getStorageTypeId();
        final int nativeDataTypeId = value.getType().getNativeTypeId();
        addAttribute(objectPath, name, storageDataTypeId, nativeDataTypeId, value.toStorageForm());
    }

    public void addTypeVariant(final String objectPath, final HDF5DataTypeVariant typeVariant)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()));
    }

    private void addTypeVariant(final int objectId, final HDF5DataTypeVariant typeVariant,
            ICleanUpRegistry registry)
    {
        addAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()), registry);
    }

    public void addStringAttribute(final String objectPath, final String name, final String value)
    {
        addStringAttribute(objectPath, name, value, value.length());
    }

    public void addStringAttribute(final String objectPath, final String name, final String value,
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
                            addStringAttribute(objectId, name, value, maxLength, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private void addStringAttribute(final int objectId, final String name, final String value,
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

    public void addBooleanAttribute(final String objectPath, final String name, final boolean value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, baseWriter.booleanDataTypeId, baseWriter.booleanDataTypeId,
                new byte[]
                    { (byte) (value ? 1 : 0) });
    }

    public void addIntAttribute(final String objectPath, final String name, final int value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_STD_I32LE, H5T_NATIVE_INT32, HDFNativeData
                .intToByte(value));
    }

    public void addLongAttribute(final String objectPath, final String name, final long value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_STD_I64LE, H5T_NATIVE_INT64, HDFNativeData
                .longToByte(value));
    }

    public void addFloatAttribute(final String objectPath, final String name, final float value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_IEEE_F32LE, H5T_NATIVE_FLOAT, HDFNativeData
                .floatToByte(value));
    }

    public void addDoubleAttribute(final String objectPath, final String name, final double value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_IEEE_F64LE, H5T_NATIVE_DOUBLE, HDFNativeData
                .doubleToByte(value));
    }

    private void addAttribute(final String objectPath, final String name,
            final int storageDataTypeId, final int nativeDataTypeId, final byte[] value)
    {
        assert objectPath != null;
        assert name != null;
        assert storageDataTypeId >= 0;
        assert nativeDataTypeId >= 0;
        assert value != null;

        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            addAttribute(objectId, name, storageDataTypeId, nativeDataTypeId,
                                    value, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    private void addAttribute(final int objectId, final String name, final int storageDataTypeId,
            final int nativeDataTypeId, final byte[] value, ICleanUpRegistry registry)
    {
        final int attributeId;
        if (baseWriter.h5.existsAttribute(objectId, name))
        {
            attributeId = baseWriter.h5.openAttribute(objectId, name, registry);
        } else
        {
            attributeId =
                    baseWriter.h5.createAttribute(objectId, name, storageDataTypeId, registry);
        }
        baseWriter.h5.writeAttribute(attributeId, nativeDataTypeId, value);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

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
                                { realLength }, HDF5GenericCompression.GENERIC_NO_COMPRESSION,
                                    registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_B64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            BitSetConversionUtils.toStorageForm(data));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeBitField(final String objectPath, final BitSet data)
    {
        writeBitField(objectPath, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeBitField(final String objectPath, final BitSet data,
            final HDF5GenericCompression compression)
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
                                { data.length }, HDF5GenericCompression.GENERIC_NO_COMPRESSION,
                                    registry);
                    H5Dwrite(dataSetId, dataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data)
    {
        writeOpaqueByteArray(objectPath, tag, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data,
            final HDF5GenericCompression compression)
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

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize)
    {
        return createOpaqueByteArray(objectPath, tag, size, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize, final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert tag != null;
        assert size >= 0;
        assert blockSize >= 0 && blockSize <= size;

        baseWriter.checkOpen();
        final int dataTypeId = getOrCreateOpaqueTypeId(tag);
        final ICallableWithCleanUp<Void> createRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, dataTypeId, compression, new long[]
                        { size }, new long[]
                        { blockSize }, false, registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - START
    // ------------------------------------------------------------------------------

    public void createByteArray(String objectPath, long size, int blockSize)
    {
        byteWriter.createByteArray(objectPath, size, blockSize);
    }

    public void createByteArray(String objectPath, long size, int blockSize,
            HDF5IntCompression compression)
    {
        byteWriter.createByteArray(objectPath, size, blockSize, compression);
    }

    public void createByteArrayCompact(String objectPath, long length)
    {
        byteWriter.createByteArrayCompact(objectPath, length);
    }

    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        byteWriter.createByteMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntCompression compression)
    {
        byteWriter.createByteMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        byteWriter.createByteMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntCompression compression)
    {
        byteWriter.createByteMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, compression);
    }

    public void writeByte(String objectPath, byte value)
    {
        byteWriter.writeByte(objectPath, value);
    }

    public void writeByteArray(String objectPath, byte[] data)
    {
        byteWriter.writeByteArray(objectPath, data);
    }

    public void writeByteArray(String objectPath, byte[] data, HDF5IntCompression compression)
    {
        byteWriter.writeByteArray(objectPath, data, compression);
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

    public void writeByteArrayCompact(String objectPath, byte[] data)
    {
        byteWriter.writeByteArrayCompact(objectPath, data);
    }

    public void writeByteMDArray(String objectPath, MDByteArray data)
    {
        byteWriter.writeByteMDArray(objectPath, data);
    }

    public void writeByteMDArray(String objectPath, MDByteArray data, HDF5IntCompression compression)
    {
        byteWriter.writeByteMDArray(objectPath, data, compression);
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

    public void writeByteMatrix(String objectPath, byte[][] data, HDF5IntCompression compression)
    {
        byteWriter.writeByteMatrix(objectPath, data, compression);
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

    public void createDoubleArray(String objectPath, long size, int blockSize)
    {
        doubleWriter.createDoubleArray(objectPath, size, blockSize);
    }

    public void createDoubleArray(String objectPath, long size, int blockSize,
            HDF5FloatCompression compression)
    {
        doubleWriter.createDoubleArray(objectPath, size, blockSize, compression);
    }

    public void createDoubleArrayCompact(String objectPath, long length)
    {
        doubleWriter.createDoubleArrayCompact(objectPath, length);
    }

    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        doubleWriter.createDoubleMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatCompression compression)
    {
        doubleWriter.createDoubleMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        doubleWriter.createDoubleMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatCompression compression)
    {
        doubleWriter.createDoubleMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY,
                compression);
    }

    public void writeDouble(String objectPath, double value)
    {
        doubleWriter.writeDouble(objectPath, value);
    }

    public void writeDoubleArray(String objectPath, double[] data)
    {
        doubleWriter.writeDoubleArray(objectPath, data);
    }

    public void writeDoubleArray(String objectPath, double[] data, HDF5FloatCompression compression)
    {
        doubleWriter.writeDoubleArray(objectPath, data, compression);
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

    public void writeDoubleArrayCompact(String objectPath, double[] data)
    {
        doubleWriter.writeDoubleArrayCompact(objectPath, data);
    }

    public void writeDoubleMDArray(String objectPath, MDDoubleArray data)
    {
        doubleWriter.writeDoubleMDArray(objectPath, data);
    }

    public void writeDoubleMDArray(String objectPath, MDDoubleArray data,
            HDF5FloatCompression compression)
    {
        doubleWriter.writeDoubleMDArray(objectPath, data, compression);
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
            HDF5FloatCompression compression)
    {
        doubleWriter.writeDoubleMatrix(objectPath, data, compression);
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

    public void createFloatArray(String objectPath, long size, int blockSize)
    {
        floatWriter.createFloatArray(objectPath, size, blockSize);
    }

    public void createFloatArray(String objectPath, long size, int blockSize,
            HDF5FloatCompression compression)
    {
        floatWriter.createFloatArray(objectPath, size, blockSize, compression);
    }

    public void createFloatArrayCompact(String objectPath, long length)
    {
        floatWriter.createFloatArrayCompact(objectPath, length);
    }

    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        floatWriter.createFloatMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatCompression compression)
    {
        floatWriter.createFloatMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        floatWriter.createFloatMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatCompression compression)
    {
        floatWriter
                .createFloatMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, compression);
    }

    public void writeFloat(String objectPath, float value)
    {
        floatWriter.writeFloat(objectPath, value);
    }

    public void writeFloatArray(String objectPath, float[] data)
    {
        floatWriter.writeFloatArray(objectPath, data);
    }

    public void writeFloatArray(String objectPath, float[] data, HDF5FloatCompression compression)
    {
        floatWriter.writeFloatArray(objectPath, data, compression);
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

    public void writeFloatArrayCompact(String objectPath, float[] data)
    {
        floatWriter.writeFloatArrayCompact(objectPath, data);
    }

    public void writeFloatMDArray(String objectPath, MDFloatArray data)
    {
        floatWriter.writeFloatMDArray(objectPath, data);
    }

    public void writeFloatMDArray(String objectPath, MDFloatArray data,
            HDF5FloatCompression compression)
    {
        floatWriter.writeFloatMDArray(objectPath, data, compression);
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

    public void writeFloatMatrix(String objectPath, float[][] data, HDF5FloatCompression compression)
    {
        floatWriter.writeFloatMatrix(objectPath, data, compression);
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

    public void createIntArray(String objectPath, long size, int blockSize)
    {
        intWriter.createIntArray(objectPath, size, blockSize);
    }

    public void createIntArray(String objectPath, long size, int blockSize,
            HDF5IntCompression compression)
    {
        intWriter.createIntArray(objectPath, size, blockSize, compression);
    }

    public void createIntArrayCompact(String objectPath, long length)
    {
        intWriter.createIntArrayCompact(objectPath, length);
    }

    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        intWriter.createIntMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntCompression compression)
    {
        intWriter.createIntMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        intWriter.createIntMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntCompression compression)
    {
        intWriter.createIntMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, compression);
    }

    public void writeInt(String objectPath, int value)
    {
        intWriter.writeInt(objectPath, value);
    }

    public void writeIntArray(String objectPath, int[] data)
    {
        intWriter.writeIntArray(objectPath, data);
    }

    public void writeIntArray(String objectPath, int[] data, HDF5IntCompression compression)
    {
        intWriter.writeIntArray(objectPath, data, compression);
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

    public void writeIntArrayCompact(String objectPath, int[] data)
    {
        intWriter.writeIntArrayCompact(objectPath, data);
    }

    public void writeIntMDArray(String objectPath, MDIntArray data)
    {
        intWriter.writeIntMDArray(objectPath, data);
    }

    public void writeIntMDArray(String objectPath, MDIntArray data, HDF5IntCompression compression)
    {
        intWriter.writeIntMDArray(objectPath, data, compression);
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

    public void writeIntMatrix(String objectPath, int[][] data, HDF5IntCompression compression)
    {
        intWriter.writeIntMatrix(objectPath, data, compression);
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

    public void createLongArray(String objectPath, long size, int blockSize)
    {
        longWriter.createLongArray(objectPath, size, blockSize);
    }

    public void createLongArray(String objectPath, long size, int blockSize,
            HDF5IntCompression compression)
    {
        longWriter.createLongArray(objectPath, size, blockSize, compression);
    }

    public void createLongArrayCompact(String objectPath, long length)
    {
        longWriter.createLongArrayCompact(objectPath, length);
    }

    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        longWriter.createLongMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntCompression compression)
    {
        longWriter.createLongMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        longWriter.createLongMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntCompression compression)
    {
        longWriter.createLongMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, compression);
    }

    public void writeLong(String objectPath, long value)
    {
        longWriter.writeLong(objectPath, value);
    }

    public void writeLongArray(String objectPath, long[] data)
    {
        longWriter.writeLongArray(objectPath, data);
    }

    public void writeLongArray(String objectPath, long[] data, HDF5IntCompression compression)
    {
        longWriter.writeLongArray(objectPath, data, compression);
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

    public void writeLongArrayCompact(String objectPath, long[] data)
    {
        longWriter.writeLongArrayCompact(objectPath, data);
    }

    public void writeLongMDArray(String objectPath, MDLongArray data)
    {
        longWriter.writeLongMDArray(objectPath, data);
    }

    public void writeLongMDArray(String objectPath, MDLongArray data, HDF5IntCompression compression)
    {
        longWriter.writeLongMDArray(objectPath, data, compression);
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

    public void writeLongMatrix(String objectPath, long[][] data, HDF5IntCompression compression)
    {
        longWriter.writeLongMatrix(objectPath, data, compression);
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

    public void createShortArray(String objectPath, long size, int blockSize)
    {
        shortWriter.createShortArray(objectPath, size, blockSize);
    }

    public void createShortArray(String objectPath, long size, int blockSize,
            HDF5IntCompression compression)
    {
        shortWriter.createShortArray(objectPath, size, blockSize, compression);
    }

    public void createShortArrayCompact(String objectPath, long length)
    {
        shortWriter.createShortArrayCompact(objectPath, length);
    }

    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        shortWriter.createShortMDArray(objectPath, dimensions, blockDimensions);
    }

    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntCompression compression)
    {
        shortWriter.createShortMDArray(objectPath, dimensions, blockDimensions, compression);
    }

    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        shortWriter.createShortMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntCompression compression)
    {
        shortWriter
                .createShortMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, compression);
    }

    public void writeShort(String objectPath, short value)
    {
        shortWriter.writeShort(objectPath, value);
    }

    public void writeShortArray(String objectPath, short[] data)
    {
        shortWriter.writeShortArray(objectPath, data);
    }

    public void writeShortArray(String objectPath, short[] data, HDF5IntCompression compression)
    {
        shortWriter.writeShortArray(objectPath, data, compression);
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

    public void writeShortArrayCompact(String objectPath, short[] data)
    {
        shortWriter.writeShortArrayCompact(objectPath, data);
    }

    public void writeShortMDArray(String objectPath, MDShortArray data)
    {
        shortWriter.writeShortMDArray(objectPath, data);
    }

    public void writeShortMDArray(String objectPath, MDShortArray data,
            HDF5IntCompression compression)
    {
        shortWriter.writeShortMDArray(objectPath, data, compression);
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

    public void writeShortMatrix(String objectPath, short[][] data, HDF5IntCompression compression)
    {
        shortWriter.writeShortMatrix(objectPath, data, compression);
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
                    addTypeVariant(dataSetId,
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
                                    HDF5GenericCompression.GENERIC_NO_COMPRESSION, new long[]
                                        { length }, null, true, registry);
                    addTypeVariant(dataSetId,
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
                                { timeStamps.length },
                                    HDF5GenericCompression.GENERIC_NO_COMPRESSION, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeStamps);
                    addTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeStampArray(final String objectPath, final long length, final int blockSize)
    {
        createTimeStampArray(objectPath, length, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void createTimeStampArray(final String objectPath, final long length,
            final int blockSize, final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, compression,
                                    new long[]
                                        { length }, new long[]
                                        { blockSize }, false, registry);
                    addTypeVariant(dataSetId,
                            HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps)
    {
        writeTimeStampArray(objectPath, timeStamps, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeTimeStampArray(final String objectPath, final long[] timeStamps,
            final HDF5GenericCompression compression)
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
                    addTypeVariant(dataSetId,
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
            final HDF5GenericCompression compression)
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
                    addTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
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
                                    HDF5GenericCompression.GENERIC_NO_COMPRESSION, new long[]
                                        { length }, null, true, registry);
                    addTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
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
                                { timeDurations.length },
                                    HDF5GenericCompression.GENERIC_NO_COMPRESSION, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            timeDurations);
                    addTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void createTimeDurationArray(final String objectPath, final long length,
            final int blockSize, final HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, length, blockSize, timeUnit,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void createTimeDurationArray(final String objectPath, final long length,
            final int blockSize, final HDF5TimeUnit timeUnit,
            final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert length > 0;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.createDataSet(objectPath, H5T_STD_I64LE, compression,
                                    new long[]
                                        { length }, new long[]
                                        { blockSize }, false, registry);
                    addTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations)
    {
        writeTimeDurationArray(objectPath, timeDurations, HDF5TimeUnit.SECONDS,
                HDF5IntCompression.INT_NO_COMPRESSION);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit)
    {
        writeTimeDurationArray(objectPath, timeDurations, timeUnit,
                HDF5IntCompression.INT_NO_COMPRESSION);
    }

    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit, final HDF5IntCompression compression)
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
                    addTypeVariant(dataSetId, timeUnit.getTypeVariant(), registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
        writeString(objectPath, data, maxLength, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data)
    {
        writeString(objectPath, data, data.length(), HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeString(final String objectPath, final String data,
            final HDF5GenericCompression compression)
    {
        writeString(objectPath, data, data.length(), compression);
    }

    public void writeString(final String objectPath, final String data, final int maxLength,
            final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> writeRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    final int definiteMaxLength = maxLength + 1;
                    final int stringDataTypeId =
                            baseWriter.h5.createDataTypeString(definiteMaxLength, registry);
                    final long[] chunkSizeOrNull =
                            HDF5Utils.tryGetChunkSizeForString(definiteMaxLength, compression
                                    .requiresChunking());
                    final int dataSetId;
                    if (exists(objectPath))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    } else
                    {
                        final StorageLayout layout =
                                baseWriter.determineLayout(stringDataTypeId,
                                        HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull, false);
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
            final HDF5GenericCompression compression)
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
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public void writeStringArray(final String objectPath, final String[] data, final int maxLength)
    {
        writeStringArray(objectPath, data, maxLength, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
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
            final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert data != null;
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
                        { data.length };
                    if (exists(objectPath))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                        // Implementation note: HDF5 1.8 seems to be able to change the size even if
                        // dimensions are not in bound of max dimensions, but the resulting file can
                        // no longer be read correctly by a HDF5 1.6.x library.
                        if (baseWriter.areDimensionsInBounds(dataSetId, dimensions))
                        {
                            baseWriter.h5.setDataSetExtent(dataSetId, dimensions);
                        }
                    } else
                    {
                        final long[] chunkSizeOrNull =
                                HDF5Utils.tryGetChunkSizeForStringVector(data.length, maxLength,
                                        compression.requiresChunking(),
                                        baseWriter.useExtentableDataTypes);
                        final StorageLayout layout =
                                baseWriter.determineLayout(stringDataTypeId, dimensions,
                                        chunkSizeOrNull, false);
                        dataSetId =
                                baseWriter.h5.createDataSet(baseWriter.fileId, dimensions,
                                        chunkSizeOrNull, stringDataTypeId, compression, objectPath,
                                        layout, baseWriter.fileFormat, registry);
                    }
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                            maxLength);
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
                    final int dataSetId;
                    if (exists(objectPath))
                    {
                        dataSetId =
                                baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
                    } else
                    {
                        dataSetId =
                                baseWriter.h5.createScalarDataSet(baseWriter.fileId,
                                        baseWriter.variableLengthStringDataTypeId, objectPath,
                                        registry);
                    }
                    H5DwriteString(dataSetId, baseWriter.variableLengthStringDataTypeId, H5S_ALL,
                            H5S_ALL, H5P_DEFAULT, new String[]
                                { data });
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
                                    HDF5GenericCompression.GENERIC_NO_COMPRESSION, registry);
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
        writeEnumArray(objectPath, data, HDF5IntCompression.INT_NO_COMPRESSION);
    }

    public void writeEnumArray(final String objectPath, final HDF5EnumerationValueArray data,
            final HDF5IntCompression compression) throws HDF5JavaException
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
                        final HDF5IntCompression actualCompression =
                                HDF5IntCompression.createDeflateAndIntegerScaling(compression
                                        .getDeflateLevel(), data.getType().getNumberOfBits());
                        final int dataSetId;
                        switch (data.getStorageForm())
                        {
                            case BYTE:
                                dataSetId =
                                        baseWriter.getDataSetId(objectPath, H5T_STD_I8LE,
                                                new long[]
                                                    { data.getLength() }, actualCompression,
                                                registry);
                                H5Dwrite(dataSetId, H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                                        data.getStorageFormBArray());
                                break;
                            case SHORT:
                                dataSetId =
                                        baseWriter.getDataSetId(objectPath, H5T_STD_I16LE,
                                                new long[]
                                                    { data.getLength() }, actualCompression,
                                                registry);
                                H5Dwrite_short(dataSetId, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL,
                                        H5P_DEFAULT, data.getStorageFormSArray());
                                break;
                            case INT:
                                dataSetId =
                                        baseWriter.getDataSetId(objectPath, H5T_STD_I32LE,
                                                new long[]
                                                    { data.getLength() }, actualCompression,
                                                registry);
                                H5Dwrite_int(dataSetId, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL,
                                        H5P_DEFAULT, data.getStorageFormIArray());
                                break;
                            default: // cannot happen but makes the compiler happy
                                dataSetId = -1;
                        }
                        addTypeVariant(dataSetId, HDF5DataTypeVariant.ENUM, registry);
                        addStringAttribute(dataSetId, HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE, data
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
        writeCompoundArrayCompact(objectPath, type, data,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundArrayCompact(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final HDF5GenericCompression compression)
    {
        writeCompoundArrayCompact(objectPath, type, data, compression, null);
    }

    public <T> void writeCompoundArrayCompact(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data,
            final HDF5GenericCompression compression, final IByteArrayInspector inspectorOrNull)
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

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data)
    {
        writeCompoundArray(objectPath, type, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericCompression compression)
    {
        writeCompoundArray(objectPath, type, data, compression, null);
    }

    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericCompression compression,
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize)
    {
        createCompoundArray(objectPath, type, size, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize, final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert type != null;
        assert blockSize >= 0;

        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    baseWriter.createDataSet(objectPath, type.getStorageTypeId(), compression,
                            new long[]
                                { size }, new long[]
                                { blockSize }, false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data)
    {
        writeCompoundMDArray(objectPath, type, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericCompression compression)
    {
        writeCompoundMDArray(objectPath, type, data, compression, null);
    }

    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericCompression compression,
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
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockDimensions[i] * dimensions[i];
        }
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                    final int dataSetId =
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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
                    final int dataSetId =
                            baseWriter.h5.openDataSet(baseWriter.fileId, objectPath, registry);
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

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions)
    {
        createCompoundMDArray(objectPath, type, dimensions, blockDimensions,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions,
            final HDF5GenericCompression compression)
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
                            dimensions, MDArray.toLong(blockDimensions), false, registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

}
