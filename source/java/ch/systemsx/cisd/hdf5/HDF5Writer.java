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

import static ch.systemsx.cisd.hdf5.HDF5Utils.createAttributeTypeVariantAttributeName;
import static ch.systemsx.cisd.hdf5.HDF5Utils.createObjectTypeVariantAttributeName;

import java.io.Flushable;
import java.util.BitSet;
import java.util.Date;
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.IHDF5CompoundInformationRetriever.IByteArrayInspector;
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

    private final IHDF5BooleanWriter booleanWriter;

    private final IHDF5StringWriter stringWriter;

    private final IHDF5EnumWriter enumWriter;

    private final IHDF5CompoundWriter compoundWriter;

    private final IHDF5DateTimeWriter dateTimeWriter;
    
    private final HDF5TimeDurationWriter timeDurationWriter;

    private final IHDF5ReferenceWriter referenceWriter;

    private final IHDF5OpaqueWriter opaqueWriter;

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
        this.booleanWriter = new HDF5BooleanWriter(baseWriter);
        this.stringWriter = new HDF5StringWriter(baseWriter);
        this.enumWriter = new HDF5EnumWriter(baseWriter);
        this.compoundWriter = new HDF5CompoundWriter(baseWriter, enumWriter);
        this.dateTimeWriter = new HDF5DateTimeWriter(baseWriter, (HDF5LongReader) longReader);
        this.timeDurationWriter = new HDF5TimeDurationWriter(baseWriter, (HDF5LongReader) longReader);
        this.referenceWriter = new HDF5ReferenceWriter(baseWriter);
        this.opaqueWriter = new HDF5OpaqueWriter(baseWriter);
    }

    HDF5BaseWriter getBaseWriter()
    {
        return baseWriter;
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    @Override
    public boolean isUseExtendableDataTypes()
    {
        return baseWriter.useExtentableDataTypes;
    }

    @Override
    public FileFormat getFileFormat()
    {
        return baseWriter.fileFormat;
    }

    // /////////////////////
    // File
    // /////////////////////

    @Override
    public void flush()
    {
        baseWriter.checkOpen();
        baseWriter.flush();
    }

    @Override
    public void flushSyncBlocking()
    {
        baseWriter.checkOpen();
        baseWriter.flushSyncBlocking();
    }

    @Override
    public boolean addFlushable(Flushable flushable)
    {
        return baseWriter.addFlushable(flushable);
    }

    @Override
    public boolean removeFlushable(Flushable flushable)
    {
        return baseWriter.removeFlushable(flushable);
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    @Override
    public void createHardLink(String currentPath, String newPath)
    {
        assert currentPath != null;
        assert newPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createHardLink(baseWriter.fileId, currentPath, newPath);
    }

    @Override
    public void createSoftLink(String targetPath, String linkPath)
    {
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createSoftLink(baseWriter.fileId, linkPath, targetPath);
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public void move(String oldLinkPath, String newLinkPath)
    {
        baseWriter.checkOpen();
        baseWriter.h5.moveLink(baseWriter.fileId, oldLinkPath, newLinkPath);
    }

    // /////////////////////
    // Group
    // /////////////////////

    @Override
    public void createGroup(final String groupPath)
    {
        baseWriter.checkOpen();
        baseWriter.h5.createGroup(baseWriter.fileId, groupPath);
    }

    @Override
    public void createGroup(final String groupPath, final int sizeHint)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> createGroupRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
                public Void call(ICleanUpRegistry registry)
                {
                    baseWriter.h5.createOldStyleGroup(baseWriter.fileId, groupPath, sizeHint,
                            registry);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(createGroupRunnable);
    }

    @Override
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
                @Override
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

    @Override
    public void deleteAttribute(final String objectPath, final String name)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> deleteAttributeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
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

    @Override
    public void setTypeVariant(final String objectPath, final HDF5DataTypeVariant typeVariant)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath,
                createObjectTypeVariantAttributeName(baseWriter.houseKeepingNameSuffix),
                baseWriter.typeVariantDataType.getStorageTypeId(),
                baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()));
    }

    @Override
    public void setTypeVariant(String objectPath, String attributeName,
            HDF5DataTypeVariant typeVariant)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(
                objectPath,
                createAttributeTypeVariantAttributeName(attributeName,
                        baseWriter.houseKeepingNameSuffix), baseWriter.typeVariantDataType
                        .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()));
    }

    @Override
    public void deleteTypeVariant(String objectPath)
    {
        deleteAttribute(objectPath,
                createObjectTypeVariantAttributeName(baseWriter.houseKeepingNameSuffix));
    }

    @Override
    public void deleteTypeVariant(String objectPath, String attributeName)
    {
        deleteAttribute(
                objectPath,
                createAttributeTypeVariantAttributeName(attributeName,
                        baseWriter.houseKeepingNameSuffix));
    }

    @Override
    public void setBooleanAttribute(String objectPath, String name, boolean value)
    {
        booleanWriter.setBooleanAttribute(objectPath, name, value);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    //
    // Generic
    //

    @Override
    public void setDataSetSize(final String objectPath, final long newSize)
    {
        setDataSetDimensions(objectPath, new long[]
            { newSize });
    }

    @Override
    public void setDataSetDimensions(final String objectPath, final long[] newDimensions)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                @Override
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

    @Override
    public void writeBitField(String objectPath, BitSet data, HDF5GenericStorageFeatures features)
    {
        booleanWriter.writeBitField(objectPath, data, features);
    }

    @Override
    public void writeBitField(String objectPath, BitSet data)
    {
        booleanWriter.writeBitField(objectPath, data);
    }

    @Override
    public void writeBoolean(String objectPath, boolean value)
    {
        booleanWriter.writeBoolean(objectPath, value);
    }

    //
    // Opaque
    //

    @Override
    public void createBitField(String objectPath, int size)
    {
        booleanWriter.createBitField(objectPath, size);
    }

    @Override
    public void createBitField(String objectPath, long size, int blockSize)
    {
        booleanWriter.createBitField(objectPath, size, blockSize);
    }

    @Override
    public void createBitField(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        booleanWriter.createBitField(objectPath, size, features);
    }

    @Override
    public void createBitField(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        booleanWriter.createBitField(objectPath, size, blockSize, features);
    }

    @Override
    public void writeBitFieldBlock(String objectPath, BitSet data, int dataSize, long blockNumber)
    {
        booleanWriter.writeBitFieldBlock(objectPath, data, dataSize, blockNumber);
    }

    @Override
    public void writeBitFieldBlockWithOffset(String objectPath, BitSet data, int dataSize,
            long offset)
    {
        booleanWriter.writeBitFieldBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, int size,
            HDF5GenericStorageFeatures features)
    {
        return opaqueWriter.createOpaqueByteArray(objectPath, tag, size, features);
    }

    @Override
    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, int size)
    {
        return opaqueWriter.createOpaqueByteArray(objectPath, tag, size);
    }

    @Override
    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, long size,
            int blockSize, HDF5GenericStorageFeatures features)
    {
        return opaqueWriter.createOpaqueByteArray(objectPath, tag, size, blockSize, features);
    }

    @Override
    public HDF5OpaqueType createOpaqueByteArray(String objectPath, String tag, long size,
            int blockSize)
    {
        return opaqueWriter.createOpaqueByteArray(objectPath, tag, size, blockSize);
    }

    @Override
    public void writeOpaqueByteArray(String objectPath, String tag, byte[] data,
            HDF5GenericStorageFeatures features)
    {
        opaqueWriter.writeOpaqueByteArray(objectPath, tag, data, features);
    }

    @Override
    public void writeOpaqueByteArray(String objectPath, String tag, byte[] data)
    {
        opaqueWriter.writeOpaqueByteArray(objectPath, tag, data);
    }

    @Override
    public void writeOpaqueByteArrayBlock(String objectPath, HDF5OpaqueType dataType, byte[] data,
            long blockNumber)
    {
        opaqueWriter.writeOpaqueByteArrayBlock(objectPath, dataType, data, blockNumber);
    }

    @Override
    public void writeOpaqueByteArrayBlockWithOffset(String objectPath, HDF5OpaqueType dataType,
            byte[] data, int dataSize, long offset)
    {
        opaqueWriter.writeOpaqueByteArrayBlockWithOffset(objectPath, dataType, data, dataSize,
                offset);
    }

    //
    // Date
    //

    @Override
    public IHDF5DateTimeWriter time()
    {
        return dateTimeWriter;
    }

    @Override
    public IHDF5TimeDurationWriter duration()
    {
        return timeDurationWriter;
    }

    @Override
    public void createTimeStampArray(String objectPath, int size,
            HDF5GenericStorageFeatures features)
    {
        dateTimeWriter.createArray(objectPath, size, features);
    }

    @Override
    public void setTimeStampAttribute(String objectPath, String name, long value)
    {
        dateTimeWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setDateAttribute(String objectPath, String name, Date date)
    {
        dateTimeWriter.setAttr(objectPath, name, date);
    }

    @Override
    public void setTimeDurationAttribute(String objectPath, String name,
            HDF5TimeDuration timeDuration)
    {
        timeDurationWriter.setAttr(objectPath, name, timeDuration);
    }

    @Override
    public void setTimeDurationAttribute(String objectPath, String name, long timeDuration,
            HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.setAttr(objectPath, name, timeDuration, timeUnit);
    }

    @Override
    public void setDateArrayAttribute(String objectPath, String name, Date[] dates)
    {
        dateTimeWriter.setArrayAttr(objectPath, name, dates);
    }

    @Override
    public void setTimeStampArrayAttribute(String objectPath, String name, long[] timeStamps)
    {
        dateTimeWriter.setArrayAttr(objectPath, name, timeStamps);
    }

    @Override
    public void setTimeDurationArrayAttribute(String objectPath, String name,
            HDF5TimeDurationArray timeDurations)
    {
        timeDurationWriter.setArrayAttr(objectPath, name, timeDurations);
    }

    @Override
    public void createTimeStampArray(String objectPath, int size)
    {
        dateTimeWriter.createArray(objectPath, size);
    }

    @Override
    public void createTimeStampArray(String objectPath, long size, int blockSize,
            HDF5GenericStorageFeatures features)
    {
        dateTimeWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createTimeStampArray(String objectPath, long size, int blockSize)
    {
        dateTimeWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void writeDate(String objectPath, Date date)
    {
        dateTimeWriter.write(objectPath, date);
    }

    @Override
    public void writeDateArray(String objectPath, Date[] dates, HDF5GenericStorageFeatures features)
    {
        dateTimeWriter.writeArray(objectPath, dates, features);
    }

    @Override
    public void writeDateArray(String objectPath, Date[] dates)
    {
        dateTimeWriter.writeArray(objectPath, dates);
    }

    @Override
    public void writeTimeStamp(String objectPath, long timeStamp)
    {
        dateTimeWriter.write(objectPath, timeStamp);
    }

    @Override
    public void writeTimeStampArray(String objectPath, long[] timeStamps,
            HDF5GenericStorageFeatures features)
    {
        dateTimeWriter.writeArray(objectPath, timeStamps, features);
    }

    @Override
    public void writeTimeStampArray(String objectPath, long[] timeStamps)
    {
        dateTimeWriter.writeArray(objectPath, timeStamps);
    }

    @Override
    public void writeTimeStampArrayBlock(String objectPath, long[] data, long blockNumber)
    {
        dateTimeWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeTimeStampArrayBlockWithOffset(String objectPath, long[] data, int dataSize,
            long offset)
    {
        dateTimeWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    //
    // Duration
    //

    @Override
    public void createTimeDurationArray(String objectPath, int size, HDF5TimeUnit timeUnit,
            HDF5GenericStorageFeatures features)
    {
        timeDurationWriter.createArray(objectPath, size, timeUnit, features);
    }

    @Override
    public void createTimeDurationArray(String objectPath, int size, HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.createArray(objectPath, size, timeUnit);
    }

    @Override
    public void createTimeDurationArray(String objectPath, long size, int blockSize,
            HDF5TimeUnit timeUnit, HDF5GenericStorageFeatures features)
    {
        timeDurationWriter.createArray(objectPath, size, blockSize, timeUnit, features);
    }

    @Override
    public void createTimeDurationArray(String objectPath, long size, int blockSize,
            HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.createArray(objectPath, size, blockSize, timeUnit);
    }

    @Override
    public void writeTimeDuration(String objectPath, long timeDuration, HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.write(objectPath, timeDuration, timeUnit);
    }

    @Override
    public void writeTimeDuration(String objectPath, HDF5TimeDuration timeDuration)
    {
        timeDurationWriter.write(objectPath, timeDuration);
    }

    @Override
    @Deprecated
    public void writeTimeDuration(String objectPath, long timeDuration)
    {
        timeDurationWriter.writeTimeDuration(objectPath, timeDuration);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArray(String objectPath, long[] timeDurations,
            HDF5TimeUnit timeUnit, HDF5IntStorageFeatures features)
    {
        timeDurationWriter.writeTimeDurationArray(objectPath, timeDurations, timeUnit, features);
    }

    @Override
    public void writeTimeDurationArray(String objectPath, HDF5TimeDurationArray timeDurations)
    {
        timeDurationWriter.writeArray(objectPath, timeDurations);
    }

    @Override
    public void writeTimeDurationArray(String objectPath, HDF5TimeDurationArray timeDurations,
            HDF5IntStorageFeatures features)
    {
        timeDurationWriter.writeArray(objectPath, timeDurations, features);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArray(String objectPath, long[] timeDurations,
            HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.writeTimeDurationArray(objectPath, timeDurations, timeUnit);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArray(String objectPath, long[] timeDurations)
    {
        timeDurationWriter.writeTimeDurationArray(objectPath, timeDurations);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArray(String objectPath, HDF5TimeDuration[] timeDurations)
    {
        timeDurationWriter.writeTimeDurationArray(objectPath, timeDurations);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArray(String objectPath, HDF5TimeDuration[] timeDurations,
            HDF5IntStorageFeatures features)
    {
        timeDurationWriter.writeTimeDurationArray(objectPath, timeDurations, features);
    }

    @Override
    public void writeTimeDurationArrayBlock(String objectPath, HDF5TimeDurationArray data,
            long blockNumber)
    {
        timeDurationWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeTimeDurationArrayBlockWithOffset(String objectPath,
            HDF5TimeDurationArray data, int dataSize, long offset)
    {
        timeDurationWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArrayBlock(String objectPath, long[] data, long blockNumber,
            HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.writeTimeDurationArrayBlock(objectPath, data, blockNumber, timeUnit);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(String objectPath, long[] data, int dataSize,
            long offset, HDF5TimeUnit timeUnit)
    {
        timeDurationWriter.writeTimeDurationArrayBlockWithOffset(objectPath, data, dataSize, offset,
                timeUnit);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArrayBlock(String objectPath, HDF5TimeDuration[] data,
            long blockNumber)
    {
        timeDurationWriter.writeTimeDurationArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(String objectPath, HDF5TimeDuration[] data,
            int dataSize, long offset)
    {
        timeDurationWriter.writeTimeDurationArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    //
    // References
    //

    @Override
    public void writeObjectReference(String objectPath, String referencedObjectPath)
    {
        referenceWriter.writeObjectReference(objectPath, referencedObjectPath);
    }

    @Override
    public void writeObjectReferenceArray(String objectPath, String[] referencedObjectPath)
    {
        referenceWriter.writeObjectReferenceArray(objectPath, referencedObjectPath);
    }

    @Override
    public void writeObjectReferenceArray(String objectPath, String[] referencedObjectPath,
            HDF5IntStorageFeatures features)
    {
        referenceWriter.writeObjectReferenceArray(objectPath, referencedObjectPath, features);
    }

    @Override
    public void writeObjectReferenceMDArray(String objectPath, MDArray<String> referencedObjectPaths)
    {
        referenceWriter.writeObjectReferenceMDArray(objectPath, referencedObjectPaths);
    }

    @Override
    public void writeObjectReferenceMDArray(String objectPath,
            MDArray<String> referencedObjectPaths, HDF5IntStorageFeatures features)
    {
        referenceWriter.writeObjectReferenceMDArray(objectPath, referencedObjectPaths, features);
    }

    @Override
    public void setObjectReferenceAttribute(String objectPath, String name,
            String referencedObjectPath)
    {
        referenceWriter.setObjectReferenceAttribute(objectPath, name, referencedObjectPath);
    }

    @Override
    public void setObjectReferenceArrayAttribute(String objectPath, String name, String[] value)
    {
        referenceWriter.setObjectReferenceArrayAttribute(objectPath, name, value);
    }

    //
    // String
    //

    @Override
    public IHDF5StringWriter string()
    {
        return stringWriter;
    }

    @Override
    public void setObjectReferenceMDArrayAttribute(String objectPath, String name,
            MDArray<String> referencedObjectPaths)
    {
        referenceWriter.setObjectReferenceMDArrayAttribute(objectPath, name, referencedObjectPaths);
    }

    @Override
    public void createObjectReferenceArray(String objectPath, int size)
    {
        referenceWriter.createObjectReferenceArray(objectPath, size);
    }

    @Override
    public void createObjectReferenceArray(String objectPath, long size, int blockSize)
    {
        referenceWriter.createObjectReferenceArray(objectPath, size, blockSize);
    }

    @Override
    public void createObjectReferenceArray(String objectPath, int size,
            HDF5IntStorageFeatures features)
    {
        referenceWriter.createObjectReferenceArray(objectPath, size, features);
    }

    @Override
    public void createObjectReferenceArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        referenceWriter.createObjectReferenceArray(objectPath, size, blockSize, features);
    }

    @Override
    public void writeObjectReferenceArrayBlock(String objectPath, String[] referencedObjectPaths,
            long blockNumber)
    {
        referenceWriter.writeObjectReferenceArrayBlock(objectPath, referencedObjectPaths,
                blockNumber);
    }

    @Override
    public void writeObjectReferenceArrayBlockWithOffset(String objectPath,
            String[] referencedObjectPaths, int dataSize, long offset)
    {
        referenceWriter.writeObjectReferenceArrayBlockWithOffset(objectPath, referencedObjectPaths,
                dataSize, offset);
    }

    @Override
    public void createObjectReferenceMDArray(String objectPath, int[] dimensions)
    {
        referenceWriter.createObjectReferenceMDArray(objectPath, dimensions);
    }

    @Override
    public void createObjectReferenceMDArray(String objectPath, long[] dimensions,
            int[] blockDimensions)
    {
        referenceWriter.createObjectReferenceMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createObjectReferenceMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        referenceWriter.createObjectReferenceMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createObjectReferenceMDArray(String objectPath, long[] dimensions,
            int[] blockDimensions, HDF5IntStorageFeatures features)
    {
        referenceWriter.createObjectReferenceMDArray(objectPath, dimensions, blockDimensions,
                features);
    }

    @Override
    public void writeObjectReferenceMDArrayBlock(String objectPath,
            MDArray<String> referencedObjectPaths, long[] blockNumber)
    {
        referenceWriter.writeObjectReferenceMDArrayBlock(objectPath, referencedObjectPaths,
                blockNumber);
    }

    @Override
    public void writeObjectReferenceMDArrayBlockWithOffset(String objectPath,
            MDArray<String> referencedObjectPaths, long[] offset)
    {
        referenceWriter.writeObjectReferenceMDArrayBlockWithOffset(objectPath,
                referencedObjectPaths, offset);
    }

    @Override
    public void writeObjectReferenceMDArrayBlockWithOffset(String objectPath, MDLongArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        referenceWriter.writeObjectReferenceMDArrayBlockWithOffset(objectPath, data,
                blockDimensions, offset, memoryOffset);
    }

    @Override
    public void createStringArray(String objectPath, int maxLength, int size)
    {
        stringWriter.createArray(objectPath, maxLength, size);
    }

    @Override
    public void createStringArray(String objectPath, int maxLength, long size, int blockSize)
    {
        stringWriter.createArray(objectPath, maxLength, size, blockSize);
    }

    @Override
    public void createStringArray(String objectPath, int maxLength, int size,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createArray(objectPath, maxLength, size, features);
    }

    @Override
    public void createStringArray(String objectPath, int maxLength, long size, int blockSize,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createArray(objectPath, maxLength, size, blockSize, features);
    }

    @Override
    public void createStringVariableLengthArray(String objectPath, int size)
    {
        stringWriter.createArrayVL(objectPath, size);
    }

    @Override
    public void createStringVariableLengthArray(String objectPath, long size, int blockSize)
    {
        stringWriter.createArrayVL(objectPath, size, blockSize);
    }

    @Override
    public void createStringVariableLengthArray(String objectPath, long size, int blockSize,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createArrayVL(objectPath, size, blockSize, features);
    }

    @Override
    public void createStringVariableLengthArray(String objectPath, int size,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createArrayVL(objectPath, size, features);
    }

    @Override
    public void setStringAttribute(String objectPath, String name, String value)
    {
        stringWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setStringAttribute(String objectPath, String name, String value, int maxLength)
    {
        stringWriter.setAttr(objectPath, name, value, maxLength);
    }

    @Override
    public void setStringArrayAttribute(String objectPath, String name, String[] value,
            int maxLength)
    {
        stringWriter.setArrayAttr(objectPath, name, value, maxLength);
    }

    @Override
    public void setStringArrayAttribute(String objectPath, String name, String[] value)
    {
        stringWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setStringMDArrayAttribute(String objectPath, String name, MDArray<String> value)
    {
        stringWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setStringMDArrayAttribute(String objectPath, String name, MDArray<String> value,
            int maxLength)
    {
        stringWriter.setMDArrayAttr(objectPath, name, value, maxLength);
    }

    @Override
    public void setStringAttributeVariableLength(String objectPath, String name, String value)
    {
        stringWriter.setAttrVL(objectPath, name, value);
    }

    @Override
    public void writeString(String objectPath, String data, int maxLength)
    {
        stringWriter.write(objectPath, data, maxLength);
    }

    @Override
    public void writeString(String objectPath, String data)
    {
        stringWriter.write(objectPath, data);
    }

    @Override
    public void writeString(String objectPath, String data, HDF5GenericStorageFeatures features)
    {
        stringWriter.write(objectPath, data, features);
    }

    @Override
    public void writeString(String objectPath, String data, int maxLength,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.write(objectPath, data, maxLength, features);
    }

    @Override
    public void writeStringArray(String objectPath, String[] data,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeStringArray(String objectPath, String[] data)
    {
        stringWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeStringArray(String objectPath, String[] data, int maxLength)
    {
        stringWriter.writeArray(objectPath, data, maxLength);
    }

    @Override
    public void writeStringArray(String objectPath, String[] data, int maxLength,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeArray(objectPath, data, maxLength, features);
    }

    @Override
    public void createStringMDArray(String objectPath, int maxLength, int[] dimensions,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createMDArray(objectPath, maxLength, dimensions, features);
    }

    @Override
    public void createStringMDArray(String objectPath, int maxLength, int[] dimensions)
    {
        stringWriter.createMDArray(objectPath, maxLength, dimensions);
    }

    @Override
    public void createStringMDArray(String objectPath, int maxLength, long[] dimensions,
            int[] blockSize, HDF5GenericStorageFeatures features)
    {
        stringWriter.createMDArray(objectPath, maxLength, dimensions, blockSize, features);
    }

    @Override
    public void createStringMDArray(String objectPath, int maxLength, long[] dimensions,
            int[] blockSize)
    {
        stringWriter.createMDArray(objectPath, maxLength, dimensions, blockSize);
    }

    @Override
    public void writeStringMDArray(String objectPath, MDArray<String> data, int maxLength)
    {
        stringWriter.writeMDArray(objectPath, data, maxLength);
    }

    @Override
    public void writeStringMDArray(String objectPath, MDArray<String> data)
    {
        stringWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeStringMDArray(String objectPath, MDArray<String> data,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeStringMDArray(String objectPath, MDArray<String> data, int maxLength,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeMDArray(objectPath, data, maxLength, features);
    }

    @Override
    public void writeStringArrayBlock(String objectPath, String[] data, long blockNumber)
    {
        stringWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeStringArrayBlockWithOffset(String objectPath, String[] data, int dataSize,
            long offset)
    {
        stringWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeStringMDArrayBlock(String objectPath, MDArray<String> data, long[] blockNumber)
    {
        stringWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeStringMDArrayBlockWithOffset(String objectPath, MDArray<String> data,
            long[] offset)
    {
        stringWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeStringVariableLength(String objectPath, String data)
    {
        stringWriter.writeVL(objectPath, data);
    }

    @Override
    public void writeStringVariableLengthArray(String objectPath, String[] data)
    {
        stringWriter.writeArrayVL(objectPath, data);
    }

    @Override
    public void writeStringVariableLengthArray(String objectPath, String[] data,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeArrayVL(objectPath, data, features);
    }

    @Override
    public void writeStringVariableLengthMDArray(String objectPath, MDArray<String> data,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.writeMDArrayVL(objectPath, data, features);
    }

    @Override
    public void writeStringVariableLengthMDArray(String objectPath, MDArray<String> data)
    {
        stringWriter.writeMDArrayVL(objectPath, data);
    }

    @Override
    public void createStringVariableLengthMDArray(String objectPath, int[] dimensions,
            HDF5GenericStorageFeatures features)
    {
        stringWriter.createMDArrayVL(objectPath, dimensions, features);
    }

    @Override
    public void createStringVariableLengthMDArray(String objectPath, int[] dimensions)
    {
        stringWriter.createMDArrayVL(objectPath, dimensions);
    }

    @Override
    public void createStringVariableLengthMDArray(String objectPath, long[] dimensions,
            int[] blockSize, HDF5GenericStorageFeatures features)
    {
        stringWriter.createMDArrayVL(objectPath, dimensions, blockSize, features);
    }

    @Override
    public void createStringVariableLengthMDArray(String objectPath, long[] dimensions,
            int[] blockSize)
    {
        stringWriter.createMDArrayVL(objectPath, dimensions, blockSize);
    }

    //
    // Enum
    //

    @Override
    public IHDF5EnumWriter enums()
    {
        return enumWriter;
    }

    @Override
    public IHDF5EnumWriter enumeration()
    {
        return enumWriter;
    }

    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return enumWriter.getType(name, values);
    }

    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values,
            final boolean check) throws HDF5JavaException
    {
        return enumWriter.getType(name, values, check);
    }

    @Override
    public HDF5EnumerationType createEnumArray(String objectPath, HDF5EnumerationType enumType,
            int size)
    {
        return enumWriter.createArray(objectPath, enumType, size);
    }

    @Override
    public HDF5EnumerationType createEnumArray(String objectPath, HDF5EnumerationType enumType,
            long size, HDF5IntStorageFeatures features)
    {
        return enumWriter.createArray(objectPath, enumType, size, features);
    }

    @Override
    public HDF5EnumerationType createEnumArray(String objectPath, HDF5EnumerationType enumType,
            long size, int blockSize, HDF5IntStorageFeatures features)
    {
        return enumWriter.createArray(objectPath, enumType, size, blockSize, features);
    }

    @Override
    public HDF5EnumerationType createEnumArray(String objectPath, HDF5EnumerationType enumType,
            long size, int blockSize)
    {
        return enumWriter.createArray(objectPath, enumType, size, blockSize);
    }

    @Override
    public void setEnumAttribute(String objectPath, String name, HDF5EnumerationValue value)
    {
        enumWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setEnumAttribute(String objectPath, String name, Enum<?> value)
            throws HDF5JavaException
    {
        enumWriter.setAttr(objectPath, name, value);
    }

    @Override
    public <T extends Enum<T>> void writeEnum(String objectPath, Enum<T> value)
            throws HDF5JavaException
    {
        enumWriter.write(objectPath, value);
    }

    @Override
    public void writeEnum(String objectPath, String[] options, String value)
    {
        enumWriter.write(objectPath, enumWriter.newAnonVal(options, value));
    }

    @Override
    public <T extends Enum<T>> void writeEnumArray(String objectPath, Enum<T>[] data)
    {
        enumWriter.writeArray(objectPath, enumWriter.newAnonArray(data));
    }

    @Override
    public void writeEnumArray(String objectPath, String[] options, String[] data)
    {
        enumWriter.writeArray(objectPath, enumWriter.newAnonArray(options, data));
    }

    @Override
    public void setEnumArrayAttribute(String objectPath, String name,
            HDF5EnumerationValueArray value)
    {
        enumWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void writeEnum(String objectPath, HDF5EnumerationValue value) throws HDF5JavaException
    {
        enumWriter.write(objectPath, value);
    }

    @Override
    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data,
            HDF5IntStorageFeatures features) throws HDF5JavaException
    {
        enumWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeEnumArray(String objectPath, HDF5EnumerationValueArray data)
            throws HDF5JavaException
    {
        enumWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeEnumArrayBlock(String objectPath, HDF5EnumerationValueArray data,
            long blockNumber)
    {
        enumWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeEnumArrayBlockWithOffset(String objectPath, HDF5EnumerationValueArray data,
            int dataSize, long offset)
    {
        enumWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    //
    // Compound
    //

    @Override
    public IHDF5CompoundWriter compounds()
    {
        return compoundWriter;
    }

    @Override
    public IHDF5CompoundWriter compound()
    {
        return compoundWriter;
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(final String name, Class<T> pojoClass,
            HDF5CompoundMemberMapping... members)
    {
        return compoundWriter.getType(name, pojoClass, members);
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> pojoClass,
            HDF5CompoundMemberMapping... members)
    {
        return compoundWriter.getType(pojoClass, members);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, Class<T> pojoClass)
    {
        return compoundWriter.getInferredType(name, pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(Class<T> pojoClass)
    {
        return compoundWriter.getInferredType(pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(final String name, T template)
    {
        return compoundWriter.getInferredType(name, template);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(T template)
    {
        return compoundWriter.getInferredType(template);
    }

    @Override
    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, int size)
    {
        compoundWriter.createArray(objectPath, type, size);
    }

    @Override
    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.createArray(objectPath, type, size, features);
    }

    @Override
    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            int blockSize, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createArray(objectPath, type, size, blockSize, features);
    }

    @Override
    public <T> void createCompoundArray(String objectPath, HDF5CompoundType<T> type, long size,
            int blockSize)
    {
        compoundWriter.createArray(objectPath, type, size, blockSize);
    }

    @Override
    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createMDArray(objectPath, type, dimensions, features);
    }

    @Override
    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            int[] dimensions)
    {
        compoundWriter.createMDArray(objectPath, type, dimensions);
    }

    @Override
    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            long[] dimensions, int[] blockDimensions, HDF5GenericStorageFeatures features)
    {
        compoundWriter.createMDArray(objectPath, type, dimensions, blockDimensions, features);
    }

    @Override
    public <T> void createCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            long[] dimensions, int[] blockDimensions)
    {
        compoundWriter.createMDArray(objectPath, type, dimensions, blockDimensions);
    }

    @Override
    public <T> void writeCompound(String objectPath, HDF5CompoundType<T> type, T data,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.write(objectPath, type, data, inspectorOrNull);
    }

    @Override
    public <T> void writeCompound(String objectPath, HDF5CompoundType<T> type, T data)
    {
        compoundWriter.write(objectPath, type, data);
    }

    @Override
    public <T> void writeCompound(String objectPath, T data)
    {
        compoundWriter.write(objectPath, data);
    }

    @Override
    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data,
            HDF5GenericStorageFeatures features, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeArray(objectPath, type, data, features, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeArray(objectPath, type, data, features);
    }

    @Override
    public <T> void writeCompoundArray(String objectPath, HDF5CompoundType<T> type, T[] data)
    {
        compoundWriter.writeArray(objectPath, type, data);
    }

    @Override
    public <T> void writeCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type, T[] data,
            long blockNumber, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeArrayBlock(objectPath, type, data, blockNumber, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type, T[] data,
            long blockNumber)
    {
        compoundWriter.writeArrayBlock(objectPath, type, data, blockNumber);
    }

    @Override
    public <T> void writeCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            T[] data, long offset, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeArrayBlockWithOffset(objectPath, type, data, offset, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            T[] data, long offset)
    {
        compoundWriter.writeArrayBlockWithOffset(objectPath, type, data, offset);
    }

    @Override
    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, HDF5GenericStorageFeatures features,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeMDArray(objectPath, type, data, features, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeMDArray(objectPath, type, data, features);
    }

    @Override
    public <T> void writeCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data)
    {
        compoundWriter.writeMDArray(objectPath, type, data);
    }

    @Override
    public <T> void writeCompoundArray(String objectPath, T[] data)
    {
        compoundWriter.writeArray(objectPath, data);
    }

    @Override
    public <T> void writeCompoundArray(String objectPath, T[] data,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeArray(objectPath, data, features);
    }

    @Override
    public <T> void writeCompoundMDArray(String objectPath, MDArray<T> data)
    {
        compoundWriter.writeMDArray(objectPath, data);
    }

    @Override
    public <T> void writeCompoundMDArray(String objectPath, MDArray<T> data,
            HDF5GenericStorageFeatures features)
    {
        compoundWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public <T> void writeCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, long[] blockDimensions, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeMDArrayBlock(objectPath, type, data, blockDimensions, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            MDArray<T> data, long[] blockDimensions)
    {
        compoundWriter.writeMDArrayBlock(objectPath, type, data, blockDimensions);
    }

    @Override
    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, int[] blockDimensions, long[] offset,
            int[] memoryOffset, IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeMDArrayBlockWithOffset(objectPath, type, data, blockDimensions, offset,
                memoryOffset, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, int[] blockDimensions, long[] offset,
            int[] memoryOffset)
    {
        compoundWriter.writeMDArrayBlockWithOffset(objectPath, type, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, long[] offset,
            IByteArrayInspector inspectorOrNull)
    {
        compoundWriter.writeMDArrayBlockWithOffset(objectPath, type, data, offset, inspectorOrNull);
    }

    @Override
    public <T> void writeCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, MDArray<T> data, long[] offset)
    {
        compoundWriter.writeMDArrayBlockWithOffset(objectPath, type, data, offset);
    }

    @Override
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(Class<T> compoundClass)
    {
        return compoundWriter.getMemberInfo(compoundClass);
    }

    @Override
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(String dataTypeName)
    {
        return compoundWriter.getMemberInfo(dataTypeName);
    }

    @Override
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(String dataSetPath)
            throws HDF5JavaException
    {
        return compoundWriter.getDataSetInfo(dataSetPath);
    }

    @Override
    public HDF5CompoundType<List<?>> getInferredCompoundType(String name, List<String> memberNames,
            List<?> template)
    {
        return compoundWriter.getInferredType(name, memberNames, template);
    }

    @Override
    public HDF5CompoundType<List<?>> getInferredCompoundType(List<String> memberNames,
            List<?> template)
    {
        return compoundWriter.getInferredType(memberNames, template);
    }

    @Override
    public HDF5CompoundType<Object[]> getInferredCompoundType(String name, String[] memberNames,
            Object[] template)
    {
        return compoundWriter.getInferredType(name, memberNames, template);
    }

    @Override
    public HDF5CompoundType<Object[]> getInferredCompoundType(String[] memberNames,
            Object[] template)
    {
        return compoundWriter.getInferredType(memberNames, template);
    }

    @Override
    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath, Class<T> pojoClass)
    {
        return compoundWriter.getDataSetType(objectPath, pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> pojoClass)
    {
        return compoundWriter.getNamedType(dataTypeName, pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> pojoClass)
    {
        return compoundWriter.getNamedType(pojoClass);
    }

    // ------------------------------------------------------------------------------
    // Primitive types - START
    // ------------------------------------------------------------------------------

    @Override
    public void createByteArray(String objectPath, int blockSize)
    {
        byteWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createByteArray(String objectPath, long size, int blockSize)
    {
        byteWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createByteArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        byteWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createByteArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createByteMDArray(String objectPath, int[] blockDimensions)
    {
        byteWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        byteWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createByteMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createByteMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        byteWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createByteMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        byteWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        byteWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createByteMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        byteWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setByteArrayAttribute(String objectPath, String name, byte[] value)
    {
        byteWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setByteAttribute(String objectPath, String name, byte value)
    {
        byteWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setByteMDArrayAttribute(String objectPath, String name, MDByteArray value)
    {
        byteWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setByteMatrixAttribute(String objectPath, String name, byte[][] value)
    {
        byteWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeByte(String objectPath, byte value)
    {
        byteWriter.write(objectPath, value);
    }

    @Override
    public void writeByteArray(String objectPath, byte[] data)
    {
        byteWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeByteArray(String objectPath, byte[] data, HDF5IntStorageFeatures features)
    {
        byteWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeByteArrayBlock(String objectPath, byte[] data, long blockNumber)
    {
        byteWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeByteArrayBlockWithOffset(String objectPath, byte[] data, int dataSize,
            long offset)
    {
        byteWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeByteMDArray(String objectPath, MDByteArray data)
    {
        byteWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeByteMDArray(String objectPath, MDByteArray data,
            HDF5IntStorageFeatures features)
    {
        byteWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeByteMDArrayBlock(String objectPath, MDByteArray data, long[] blockNumber)
    {
        byteWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeByteMDArrayBlockWithOffset(String objectPath, MDByteArray data, long[] offset)
    {
        byteWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeByteMDArrayBlockWithOffset(String objectPath, MDByteArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        byteWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeByteMatrix(String objectPath, byte[][] data)
    {
        byteWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeByteMatrix(String objectPath, byte[][] data, HDF5IntStorageFeatures features)
    {
        byteWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeByteMatrixBlock(String objectPath, byte[][] data, long blockNumberX,
            long blockNumberY)
    {
        byteWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeByteMatrixBlockWithOffset(String objectPath, byte[][] data, long offsetX,
            long offsetY)
    {
        byteWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeByteMatrixBlockWithOffset(String objectPath, byte[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        byteWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    @Override
    public void createDoubleArray(String objectPath, int blockSize)
    {
        doubleWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createDoubleArray(String objectPath, long size, int blockSize)
    {
        doubleWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createDoubleArray(String objectPath, int size, HDF5FloatStorageFeatures features)
    {
        doubleWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createDoubleArray(String objectPath, long size, int blockSize,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createDoubleMDArray(String objectPath, int[] blockDimensions)
    {
        doubleWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        doubleWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createDoubleMDArray(String objectPath, int[] dimensions,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createDoubleMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createDoubleMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        doubleWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        doubleWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createDoubleMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatStorageFeatures features)
    {
        doubleWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setDoubleArrayAttribute(String objectPath, String name, double[] value)
    {
        doubleWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setDoubleAttribute(String objectPath, String name, double value)
    {
        doubleWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setDoubleMDArrayAttribute(String objectPath, String name, MDDoubleArray value)
    {
        doubleWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setDoubleMatrixAttribute(String objectPath, String name, double[][] value)
    {
        doubleWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeDouble(String objectPath, double value)
    {
        doubleWriter.write(objectPath, value);
    }

    @Override
    public void writeDoubleArray(String objectPath, double[] data)
    {
        doubleWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeDoubleArray(String objectPath, double[] data, HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeDoubleArrayBlock(String objectPath, double[] data, long blockNumber)
    {
        doubleWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeDoubleArrayBlockWithOffset(String objectPath, double[] data, int dataSize,
            long offset)
    {
        doubleWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeDoubleMDArray(String objectPath, MDDoubleArray data)
    {
        doubleWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeDoubleMDArray(String objectPath, MDDoubleArray data,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeDoubleMDArrayBlock(String objectPath, MDDoubleArray data, long[] blockNumber)
    {
        doubleWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray data,
            long[] offset)
    {
        doubleWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        doubleWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeDoubleMatrix(String objectPath, double[][] data)
    {
        doubleWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeDoubleMatrix(String objectPath, double[][] data,
            HDF5FloatStorageFeatures features)
    {
        doubleWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeDoubleMatrixBlock(String objectPath, double[][] data, long blockNumberX,
            long blockNumberY)
    {
        doubleWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeDoubleMatrixBlockWithOffset(String objectPath, double[][] data, long offsetX,
            long offsetY)
    {
        doubleWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeDoubleMatrixBlockWithOffset(String objectPath, double[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        doubleWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    @Override
    public void createFloatArray(String objectPath, int blockSize)
    {
        floatWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createFloatArray(String objectPath, long size, int blockSize)
    {
        floatWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createFloatArray(String objectPath, int size, HDF5FloatStorageFeatures features)
    {
        floatWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createFloatArray(String objectPath, long size, int blockSize,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createFloatMDArray(String objectPath, int[] blockDimensions)
    {
        floatWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        floatWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createFloatMDArray(String objectPath, int[] dimensions,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createFloatMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createFloatMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        floatWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        floatWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createFloatMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5FloatStorageFeatures features)
    {
        floatWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setFloatArrayAttribute(String objectPath, String name, float[] value)
    {
        floatWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setFloatAttribute(String objectPath, String name, float value)
    {
        floatWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setFloatMDArrayAttribute(String objectPath, String name, MDFloatArray value)
    {
        floatWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setFloatMatrixAttribute(String objectPath, String name, float[][] value)
    {
        floatWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeFloat(String objectPath, float value)
    {
        floatWriter.write(objectPath, value);
    }

    @Override
    public void writeFloatArray(String objectPath, float[] data)
    {
        floatWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeFloatArray(String objectPath, float[] data, HDF5FloatStorageFeatures features)
    {
        floatWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeFloatArrayBlock(String objectPath, float[] data, long blockNumber)
    {
        floatWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeFloatArrayBlockWithOffset(String objectPath, float[] data, int dataSize,
            long offset)
    {
        floatWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeFloatMDArray(String objectPath, MDFloatArray data)
    {
        floatWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeFloatMDArray(String objectPath, MDFloatArray data,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeFloatMDArrayBlock(String objectPath, MDFloatArray data, long[] blockNumber)
    {
        floatWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray data, long[] offset)
    {
        floatWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        floatWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeFloatMatrix(String objectPath, float[][] data)
    {
        floatWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeFloatMatrix(String objectPath, float[][] data,
            HDF5FloatStorageFeatures features)
    {
        floatWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeFloatMatrixBlock(String objectPath, float[][] data, long blockNumberX,
            long blockNumberY)
    {
        floatWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeFloatMatrixBlockWithOffset(String objectPath, float[][] data, long offsetX,
            long offsetY)
    {
        floatWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeFloatMatrixBlockWithOffset(String objectPath, float[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        floatWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    @Override
    public void createIntArray(String objectPath, int blockSize)
    {
        intWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createIntArray(String objectPath, long size, int blockSize)
    {
        intWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createIntArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        intWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createIntArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        intWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createIntMDArray(String objectPath, int[] blockDimensions)
    {
        intWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        intWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createIntMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        intWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createIntMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        intWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createIntMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        intWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        intWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createIntMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        intWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setIntArrayAttribute(String objectPath, String name, int[] value)
    {
        intWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setIntAttribute(String objectPath, String name, int value)
    {
        intWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setIntMDArrayAttribute(String objectPath, String name, MDIntArray value)
    {
        intWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setIntMatrixAttribute(String objectPath, String name, int[][] value)
    {
        intWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeInt(String objectPath, int value)
    {
        intWriter.write(objectPath, value);
    }

    @Override
    public void writeIntArray(String objectPath, int[] data)
    {
        intWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeIntArray(String objectPath, int[] data, HDF5IntStorageFeatures features)
    {
        intWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeIntArrayBlock(String objectPath, int[] data, long blockNumber)
    {
        intWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeIntArrayBlockWithOffset(String objectPath, int[] data, int dataSize,
            long offset)
    {
        intWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeIntMDArray(String objectPath, MDIntArray data)
    {
        intWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeIntMDArray(String objectPath, MDIntArray data, HDF5IntStorageFeatures features)
    {
        intWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeIntMDArrayBlock(String objectPath, MDIntArray data, long[] blockNumber)
    {
        intWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeIntMDArrayBlockWithOffset(String objectPath, MDIntArray data, long[] offset)
    {
        intWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeIntMDArrayBlockWithOffset(String objectPath, MDIntArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        intWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeIntMatrix(String objectPath, int[][] data)
    {
        intWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeIntMatrix(String objectPath, int[][] data, HDF5IntStorageFeatures features)
    {
        intWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeIntMatrixBlock(String objectPath, int[][] data, long blockNumberX,
            long blockNumberY)
    {
        intWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeIntMatrixBlockWithOffset(String objectPath, int[][] data, long offsetX,
            long offsetY)
    {
        intWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeIntMatrixBlockWithOffset(String objectPath, int[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        intWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    @Override
    public void createLongArray(String objectPath, int blockSize)
    {
        longWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createLongArray(String objectPath, long size, int blockSize)
    {
        longWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createLongArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        longWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createLongArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        longWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createLongMDArray(String objectPath, int[] blockDimensions)
    {
        longWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        longWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createLongMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        longWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createLongMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        longWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createLongMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        longWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        longWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createLongMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        longWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setLongArrayAttribute(String objectPath, String name, long[] value)
    {
        longWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setLongAttribute(String objectPath, String name, long value)
    {
        longWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setLongMDArrayAttribute(String objectPath, String name, MDLongArray value)
    {
        longWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setLongMatrixAttribute(String objectPath, String name, long[][] value)
    {
        longWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeLong(String objectPath, long value)
    {
        longWriter.write(objectPath, value);
    }

    @Override
    public void writeLongArray(String objectPath, long[] data)
    {
        longWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeLongArray(String objectPath, long[] data, HDF5IntStorageFeatures features)
    {
        longWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeLongArrayBlock(String objectPath, long[] data, long blockNumber)
    {
        longWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeLongArrayBlockWithOffset(String objectPath, long[] data, int dataSize,
            long offset)
    {
        longWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeLongMDArray(String objectPath, MDLongArray data)
    {
        longWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeLongMDArray(String objectPath, MDLongArray data,
            HDF5IntStorageFeatures features)
    {
        longWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeLongMDArrayBlock(String objectPath, MDLongArray data, long[] blockNumber)
    {
        longWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeLongMDArrayBlockWithOffset(String objectPath, MDLongArray data, long[] offset)
    {
        longWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeLongMDArrayBlockWithOffset(String objectPath, MDLongArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        longWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeLongMatrix(String objectPath, long[][] data)
    {
        longWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeLongMatrix(String objectPath, long[][] data, HDF5IntStorageFeatures features)
    {
        longWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeLongMatrixBlock(String objectPath, long[][] data, long blockNumberX,
            long blockNumberY)
    {
        longWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeLongMatrixBlockWithOffset(String objectPath, long[][] data, long offsetX,
            long offsetY)
    {
        longWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeLongMatrixBlockWithOffset(String objectPath, long[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        longWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY, offsetX,
                offsetY);
    }

    @Override
    public void createShortArray(String objectPath, int blockSize)
    {
        shortWriter.createArray(objectPath, blockSize);
    }

    @Override
    public void createShortArray(String objectPath, long size, int blockSize)
    {
        shortWriter.createArray(objectPath, size, blockSize);
    }

    @Override
    public void createShortArray(String objectPath, int size, HDF5IntStorageFeatures features)
    {
        shortWriter.createArray(objectPath, size, features);
    }

    @Override
    public void createShortArray(String objectPath, long size, int blockSize,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createArray(objectPath, size, blockSize, features);
    }

    @Override
    public void createShortMDArray(String objectPath, int[] blockDimensions)
    {
        shortWriter.createMDArray(objectPath, blockDimensions);
    }

    @Override
    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions)
    {
        shortWriter.createMDArray(objectPath, dimensions, blockDimensions);
    }

    @Override
    public void createShortMDArray(String objectPath, int[] dimensions,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createMDArray(objectPath, dimensions, features);
    }

    @Override
    public void createShortMDArray(String objectPath, long[] dimensions, int[] blockDimensions,
            HDF5IntStorageFeatures features)
    {
        shortWriter.createMDArray(objectPath, dimensions, blockDimensions, features);
    }

    @Override
    public void createShortMatrix(String objectPath, int blockSizeX, int blockSizeY)
    {
        shortWriter.createMatrix(objectPath, blockSizeX, blockSizeY);
    }

    @Override
    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY)
    {
        shortWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY);
    }

    @Override
    public void createShortMatrix(String objectPath, long sizeX, long sizeY, int blockSizeX,
            int blockSizeY, HDF5IntStorageFeatures features)
    {
        shortWriter.createMatrix(objectPath, sizeX, sizeY, blockSizeX, blockSizeY, features);
    }

    @Override
    public void setShortArrayAttribute(String objectPath, String name, short[] value)
    {
        shortWriter.setArrayAttr(objectPath, name, value);
    }

    @Override
    public void setShortAttribute(String objectPath, String name, short value)
    {
        shortWriter.setAttr(objectPath, name, value);
    }

    @Override
    public void setShortMDArrayAttribute(String objectPath, String name, MDShortArray value)
    {
        shortWriter.setMDArrayAttr(objectPath, name, value);
    }

    @Override
    public void setShortMatrixAttribute(String objectPath, String name, short[][] value)
    {
        shortWriter.setMatrixAttr(objectPath, name, value);
    }

    @Override
    public void writeShort(String objectPath, short value)
    {
        shortWriter.write(objectPath, value);
    }

    @Override
    public void writeShortArray(String objectPath, short[] data)
    {
        shortWriter.writeArray(objectPath, data);
    }

    @Override
    public void writeShortArray(String objectPath, short[] data, HDF5IntStorageFeatures features)
    {
        shortWriter.writeArray(objectPath, data, features);
    }

    @Override
    public void writeShortArrayBlock(String objectPath, short[] data, long blockNumber)
    {
        shortWriter.writeArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeShortArrayBlockWithOffset(String objectPath, short[] data, int dataSize,
            long offset)
    {
        shortWriter.writeArrayBlockWithOffset(objectPath, data, dataSize, offset);
    }

    @Override
    public void writeShortMDArray(String objectPath, MDShortArray data)
    {
        shortWriter.writeMDArray(objectPath, data);
    }

    @Override
    public void writeShortMDArray(String objectPath, MDShortArray data,
            HDF5IntStorageFeatures features)
    {
        shortWriter.writeMDArray(objectPath, data, features);
    }

    @Override
    public void writeShortMDArrayBlock(String objectPath, MDShortArray data, long[] blockNumber)
    {
        shortWriter.writeMDArrayBlock(objectPath, data, blockNumber);
    }

    @Override
    public void writeShortMDArrayBlockWithOffset(String objectPath, MDShortArray data, long[] offset)
    {
        shortWriter.writeMDArrayBlockWithOffset(objectPath, data, offset);
    }

    @Override
    public void writeShortMDArrayBlockWithOffset(String objectPath, MDShortArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        shortWriter.writeMDArrayBlockWithOffset(objectPath, data, blockDimensions, offset,
                memoryOffset);
    }

    @Override
    public void writeShortMatrix(String objectPath, short[][] data)
    {
        shortWriter.writeMatrix(objectPath, data);
    }

    @Override
    public void writeShortMatrix(String objectPath, short[][] data, HDF5IntStorageFeatures features)
    {
        shortWriter.writeMatrix(objectPath, data, features);
    }

    @Override
    public void writeShortMatrixBlock(String objectPath, short[][] data, long blockNumberX,
            long blockNumberY)
    {
        shortWriter.writeMatrixBlock(objectPath, data, blockNumberX, blockNumberY);
    }

    @Override
    public void writeShortMatrixBlockWithOffset(String objectPath, short[][] data, long offsetX,
            long offsetY)
    {
        shortWriter.writeMatrixBlockWithOffset(objectPath, data, offsetX, offsetY);
    }

    @Override
    public void writeShortMatrixBlockWithOffset(String objectPath, short[][] data, int dataSizeX,
            int dataSizeY, long offsetX, long offsetY)
    {
        shortWriter.writeMatrixBlockWithOffset(objectPath, data, dataSizeX, dataSizeY,
                offsetX, offsetY);
    }

    @Override
    public IHDF5ByteWriter int8()
    {
        return byteWriter;
    }

    @Override
    public IHDF5ShortWriter int16()
    {
        return shortWriter;
    }

    @Override
    public IHDF5IntWriter int32()
    {
        return intWriter;
    }

    @Override
    public IHDF5LongWriter int64()
    {
        return longWriter;
    }

    @Override
    public IHDF5FloatWriter float32()
    {
        return floatWriter;
    }

    @Override
    public IHDF5DoubleWriter float64()
    {
        return doubleWriter;
    }

    // ------------------------------------------------------------------------------
    // Primitive Types - END
    // ------------------------------------------------------------------------------
}
