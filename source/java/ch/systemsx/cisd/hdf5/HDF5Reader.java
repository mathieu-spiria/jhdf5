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

import static ch.systemsx.cisd.hdf5.HDF5Utils.removeInternalNames;

import java.io.File;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDAbstractArray;
import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation.DataTypeInfoOptions;
import ch.systemsx.cisd.hdf5.IHDF5CompoundInformationRetriever.IByteArrayInspector;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * A class for reading HDF5 files (HDF5 1.8.x and older).
 * <p>
 * The class focuses on ease of use instead of completeness. As a consequence not all features of a
 * valid HDF5 files can be read using this class, but only a subset. (All information written by
 * {@link HDF5Writer} can be read by this class.)
 * <p>
 * Usage:
 * 
 * <pre>
 * HDF5Reader reader = new HDF5ReaderConfig(&quot;test.h5&quot;).reader();
 * float[] f = reader.readFloatArray(&quot;/some/path/dataset&quot;);
 * String s = reader.getAttributeString(&quot;/some/path/dataset&quot;, &quot;some key&quot;);
 * reader.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
class HDF5Reader implements IHDF5Reader
{
    private final HDF5BaseReader baseReader;

    private final IHDF5ByteReader byteReader;

    private final IHDF5ShortReader shortReader;

    private final IHDF5IntReader intReader;

    private final IHDF5LongReader longReader;

    private final IHDF5FloatReader floatReader;

    private final IHDF5DoubleReader doubleReader;

    private final IHDF5BooleanReader booleanReader;

    private final IHDF5StringReader stringReader;

    private final IHDF5EnumReader enumReader;

    private final IHDF5CompoundReader compoundReader;

    private final IHDF5DateTimeReader dateTimeReader;

    private final IHDF5ReferenceReader referenceReader;

    private final IHDF5GenericReader genericReader;

    HDF5Reader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
        this.byteReader = new HDF5ByteReader(baseReader);
        this.shortReader = new HDF5ShortReader(baseReader);
        this.intReader = new HDF5IntReader(baseReader);
        this.longReader = new HDF5LongReader(baseReader);
        this.floatReader = new HDF5FloatReader(baseReader);
        this.doubleReader = new HDF5DoubleReader(baseReader);
        this.booleanReader = new HDF5BooleanReader(baseReader);
        this.stringReader = new HDF5StringReader(baseReader);
        this.enumReader = new HDF5EnumReader(baseReader);
        this.compoundReader = new HDF5CompoundReader(baseReader, enumReader);
        this.dateTimeReader = new HDF5DateTimeReader(baseReader);
        this.referenceReader = new HDF5ReferenceReader(baseReader);
        this.genericReader = new HDF5GenericReader(baseReader);
    }

    void checkOpen()
    {
        baseReader.checkOpen();
    }

    int getFileId()
    {
        return baseReader.fileId;
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    @Override
    public boolean isPerformNumericConversions()
    {
        return baseReader.performNumericConversions;
    }

    @Override
    public File getFile()
    {
        return baseReader.hdf5File;
    }

    // /////////////////////
    // Closing
    // /////////////////////

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        close();
    }

    @Override
    public void close()
    {
        baseReader.close();
    }

    @Override
    public boolean isClosed()
    {
        return baseReader.isClosed();
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    @Override
    public HDF5LinkInformation getLinkInformation(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getLinkInfo(baseReader.fileId, objectPath, false);
    }

    @Override
    public HDF5ObjectInformation getObjectInformation(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getObjectInfo(baseReader.fileId, objectPath, false);
    }

    @Override
    public HDF5ObjectType getObjectType(final String objectPath, boolean followLink)
    {
        baseReader.checkOpen();
        if (followLink)
        {
            return baseReader.h5.getObjectTypeInfo(baseReader.fileId, objectPath, false);
        } else
        {
            return baseReader.h5.getLinkTypeInfo(baseReader.fileId, objectPath, false);
        }
    }

    @Override
    public HDF5ObjectType getObjectType(final String objectPath)
    {
        return getObjectType(objectPath, true);
    }

    @Override
    public boolean exists(final String objectPath, boolean followLink)
    {
        if (followLink == false)
        {
            // Optimization
            baseReader.checkOpen();
            if ("/".equals(objectPath))
            {
                return true;
            }
            return baseReader.h5.exists(baseReader.fileId, objectPath);
        } else
        {
            return exists(objectPath);
        }
    }

    @Override
    public boolean exists(final String objectPath)
    {
        baseReader.checkOpen();
        if ("/".equals(objectPath))
        {
            return true;
        }
        return baseReader.h5.getObjectTypeId(baseReader.fileId, objectPath, false) >= 0;
    }

    @Override
    public String getHouseKeepingNameSuffix()
    {
        return baseReader.houseKeepingNameSuffix;
    }

    @Override
    public String toHouseKeepingPath(String objectPath)
    {
        return HDF5Utils.toHouseKeepingPath(objectPath, baseReader.houseKeepingNameSuffix);
    }

    @Override
    public boolean isGroup(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isGroup(getObjectType(objectPath, followLink));
    }

    @Override
    public boolean isGroup(final String objectPath)
    {
        return HDF5ObjectType.isGroup(getObjectType(objectPath));
    }

    @Override
    public boolean isDataSet(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isDataSet(getObjectType(objectPath, followLink));
    }

    @Override
    public boolean isDataSet(final String objectPath)
    {
        return HDF5ObjectType.isDataSet(getObjectType(objectPath));
    }

    @Override
    public boolean isDataType(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isDataType(getObjectType(objectPath, followLink));
    }

    @Override
    public boolean isDataType(final String objectPath)
    {
        return HDF5ObjectType.isDataType(getObjectType(objectPath));
    }

    @Override
    public boolean isSoftLink(final String objectPath)
    {
        return HDF5ObjectType.isSoftLink(getObjectType(objectPath, false));
    }

    @Override
    public boolean isExternalLink(final String objectPath)
    {
        return HDF5ObjectType.isExternalLink(getObjectType(objectPath, false));
    }

    @Override
    public boolean isSymbolicLink(final String objectPath)
    {
        return HDF5ObjectType.isSymbolicLink(getObjectType(objectPath, false));
    }

    @Override
    public String tryGetSymbolicLinkTarget(final String objectPath)
    {
        return getLinkInformation(objectPath).tryGetSymbolicLinkTarget();
    }

    @Override
    public String tryGetDataTypePath(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> dataTypeNameCallable =
                new ICallableWithCleanUp<String>()
                    {
                        @Override
                        public String call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int dataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            return baseReader.tryGetDataTypePath(dataTypeId);
                        }
                    };
        return baseReader.runner.call(dataTypeNameCallable);
    }

    @Override
    public String tryGetDataTypePath(HDF5DataType type)
    {
        assert type != null;

        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return baseReader.tryGetDataTypePath(type.getStorageTypeId());
    }

    @Override
    public List<String> getAttributeNames(final String objectPath)
    {
        assert objectPath != null;
        baseReader.checkOpen();
        return removeInternalNames(getAllAttributeNames(objectPath),
                baseReader.houseKeepingNameSuffix, "/".equals(objectPath));
    }

    @Override
    public List<String> getAllAttributeNames(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<List<String>> attributeNameReaderRunnable =
                new ICallableWithCleanUp<List<String>>()
                    {
                        @Override
                        public List<String> call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return baseReader.h5.getAttributeNames(objectId, registry);
                        }
                    };
        return baseReader.runner.call(attributeNameReaderRunnable);
    }

    @Override
    public HDF5DataTypeInformation getAttributeInformation(final String dataSetPath,
            final String attributeName)
    {
        return getAttributeInformation(dataSetPath, attributeName, DataTypeInfoOptions.DEFAULT);
    }

    @Override
    public HDF5DataTypeInformation getAttributeInformation(final String dataSetPath,
            final String attributeName, final DataTypeInfoOptions dataTypeInfoOptions)
    {
        assert dataSetPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5DataTypeInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataTypeInformation>()
                    {
                        @Override
                        public HDF5DataTypeInformation call(ICleanUpRegistry registry)
                        {
                            try
                            {
                                final int objectId =
                                        baseReader.h5.openObject(baseReader.fileId, dataSetPath,
                                                registry);
                                final int attributeId =
                                        baseReader.h5.openAttribute(objectId, attributeName,
                                                registry);
                                final int dataTypeId =
                                        baseReader.h5
                                                .getDataTypeForAttribute(attributeId, registry);
                                final HDF5DataTypeInformation dataTypeInformation =
                                        baseReader.getDataTypeInformation(dataTypeId,
                                                dataTypeInfoOptions, registry);
                                if (dataTypeInformation.isArrayType() == false)
                                {
                                    final int[] dimensions =
                                            MDAbstractArray.toInt(baseReader.h5
                                                    .getDataDimensionsForAttribute(attributeId,
                                                            registry));
                                    if (dimensions.length > 0)
                                    {
                                        dataTypeInformation.setDimensions(dimensions);
                                    }
                                }
                                return dataTypeInformation;
                            } catch (RuntimeException ex)
                            {
                                throw ex;
                            }
                        }
                    };
        return baseReader.runner.call(informationDeterminationRunnable);
    }

    @Override
    public HDF5DataSetInformation getDataSetInformation(final String dataSetPath)
    {
        return getDataSetInformation(dataSetPath, DataTypeInfoOptions.DEFAULT);
    }

    @Override
    public HDF5DataSetInformation getDataSetInformation(final String dataSetPath,
            final DataTypeInfoOptions dataTypeInfoOptions)
    {
        assert dataSetPath != null;

        baseReader.checkOpen();
        return baseReader.getDataSetInformation(dataSetPath, dataTypeInfoOptions);
    }

    @Override
    public long getSize(final String objectPath)
    {
        return getDataSetInformation(objectPath, DataTypeInfoOptions.MINIMAL).getSize();
    }

    @Override
    public long getNumberOfElements(final String objectPath)
    {
        return getDataSetInformation(objectPath, DataTypeInfoOptions.MINIMAL).getNumberOfElements();
    }

    // /////////////////////
    // Copies
    // /////////////////////

    @Override
    public void copy(final String sourceObject, final IHDF5Writer destinationWriter,
            final String destinationObject)
    {
        baseReader.checkOpen();
        final HDF5Writer dwriter = (HDF5Writer) destinationWriter;
        if (dwriter != this)
        {
            dwriter.checkOpen();
        }
        baseReader.copyObject(sourceObject, dwriter.getFileId(), destinationObject);
    }

    @Override
    public void copy(String sourceObject, IHDF5Writer destinationWriter)
    {
        copy(sourceObject, destinationWriter, "/");
    }

    @Override
    public void copyAll(IHDF5Writer destinationWriter)
    {
        copy("/", destinationWriter, "/");
    }

    // /////////////////////
    // Group
    // /////////////////////

    @Override
    public List<String> getGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMembers(groupPath);
    }

    @Override
    public List<String> getAllGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getAllGroupMembers(groupPath);
    }

    @Override
    public List<String> getGroupMemberPaths(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMemberPaths(groupPath);
    }

    @Override
    public List<HDF5LinkInformation> getGroupMemberInformation(final String groupPath,
            boolean readLinkTargets)
    {
        baseReader.checkOpen();
        if (readLinkTargets)
        {
            return baseReader.h5.getGroupMemberLinkInfo(baseReader.fileId, groupPath, false,
                    baseReader.houseKeepingNameSuffix);
        } else
        {
            return baseReader.h5.getGroupMemberTypeInfo(baseReader.fileId, groupPath, false,
                    baseReader.houseKeepingNameSuffix);
        }
    }

    @Override
    public List<HDF5LinkInformation> getAllGroupMemberInformation(final String groupPath,
            boolean readLinkTargets)
    {
        baseReader.checkOpen();
        if (readLinkTargets)
        {
            return baseReader.h5.getGroupMemberLinkInfo(baseReader.fileId, groupPath, true,
                    baseReader.houseKeepingNameSuffix);
        } else
        {
            return baseReader.h5.getGroupMemberTypeInfo(baseReader.fileId, groupPath, true,
                    baseReader.houseKeepingNameSuffix);
        }
    }

    // /////////////////////
    // Types
    // /////////////////////

    @Override
    public String tryGetOpaqueTag(String objectPath)
    {
        return genericReader.tryGetOpaqueTag(objectPath);
    }

    @Override
    public HDF5OpaqueType tryGetOpaqueType(String objectPath)
    {
        return genericReader.tryGetOpaqueType(objectPath);
    }

    @Override
    public HDF5DataTypeVariant tryGetTypeVariant(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.tryGetTypeVariant(objectPath);
    }

    @Override
    public HDF5DataTypeVariant tryGetTypeVariant(String objectPath, String attributeName)
    {
        baseReader.checkOpen();
        return baseReader.tryGetTypeVariant(objectPath, attributeName);
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    @Override
    public boolean hasAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                @Override
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.h5.existsAttribute(objectId, attributeName);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    @Override
    public boolean getBooleanAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return booleanReader.getBooleanAttribute(objectPath, attributeName);
    }

    @Override
    public String getEnumAttributeAsString(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        return enumReader.getAttrAsString(objectPath, attributeName);
    }

    @Override
    public HDF5EnumerationValue getEnumAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        return enumReader.getAttr(objectPath, attributeName);
    }

    @Override
    public <T extends Enum<T>> T getEnumAttribute(String objectPath, String attributeName,
            Class<T> enumClass) throws HDF5JavaException
    {
        return enumReader.getAttr(objectPath, attributeName, enumClass);
    }

    @Override
    public String[] getEnumArrayAttributeAsString(final String objectPath,
            final String attributeName) throws HDF5JavaException
    {
        return enumReader.getArrayAttr(objectPath, attributeName).toStringArray();
    }

    @Override
    public HDF5EnumerationValueArray getEnumArrayAttribute(final String objectPath,
            final String attributeName) throws HDF5JavaException
    {
        return enumReader.getArrayAttr(objectPath, attributeName);
    }

    @Override
    public HDF5EnumerationType getEnumType(String dataTypeName)
    {
        return enumReader.getType(dataTypeName);
    }

    @Override
    public HDF5EnumerationType getEnumType(String dataTypeName, String[] values)
            throws HDF5JavaException
    {
        return enumReader.getType(dataTypeName, values);
    }

    @Override
    public HDF5EnumerationType getEnumType(String dataTypeName, String[] values, boolean check)
            throws HDF5JavaException
    {
        return enumReader.getType(dataTypeName, values, check);
    }

    @Override
    public HDF5EnumerationType getDataSetEnumType(String dataSetPath)
    {
        return enumReader.getDataSetType(dataSetPath);
    }

    @Override
    public HDF5EnumerationType getEnumTypeForObject(String dataSetPath)
    {
        return enumReader.getDataSetType(dataSetPath);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    //
    // Generic
    //

    @Override
    public Iterable<HDF5DataBlock<byte[]>> getAsByteArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return genericReader.getAsByteArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public byte[] readAsByteArray(String objectPath)
    {
        return genericReader.readAsByteArray(objectPath);
    }

    @Override
    public byte[] getAttributeAsByteArray(String objectPath, String attributeName)
    {
        return genericReader.getAttributeAsByteArray(objectPath, attributeName);
    }

    @Override
    public byte[] readAsByteArrayBlock(String objectPath, int blockSize, long blockNumber)
            throws HDF5JavaException
    {
        return genericReader.readAsByteArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public byte[] readAsByteArrayBlockWithOffset(String objectPath, int blockSize, long offset)
            throws HDF5JavaException
    {
        return genericReader.readAsByteArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public int readAsByteArrayToBlockWithOffset(String objectPath, byte[] buffer, int blockSize,
            long offset, int memoryOffset) throws HDF5JavaException
    {
        return genericReader.readAsByteArrayToBlockWithOffset(objectPath, buffer, blockSize,
                offset, memoryOffset);
    }

    //
    // Boolean
    //

    @Override
    public BitSet readBitField(String objectPath) throws HDF5DatatypeInterfaceException
    {
        return booleanReader.readBitField(objectPath);
    }

    @Override
    public BitSet readBitFieldBlock(String objectPath, int blockSize, long blockNumber)
    {
        return booleanReader.readBitFieldBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public BitSet readBitFieldBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return booleanReader.readBitFieldBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public boolean isBitSetInBitField(String objectPath, int bitIndex)
    {
        return booleanReader.isBitSetInBitField(objectPath, bitIndex);
    }

    @Override
    public boolean readBoolean(String objectPath) throws HDF5JavaException
    {
        return booleanReader.readBoolean(objectPath);
    }

    //
    // Time & date
    //

    @Override
    public long getTimeStampAttribute(String objectPath, String attributeName)
    {
        return dateTimeReader.getTimeStampAttribute(objectPath, attributeName);
    }

    @Override
    public Date getDateAttribute(String objectPath, String attributeName)
    {
        return dateTimeReader.getDateAttribute(objectPath, attributeName);
    }

    @Override
    public boolean isTimeStamp(String objectPath, String attributeName) throws HDF5JavaException
    {
        return dateTimeReader.isTimeStamp(objectPath, attributeName);
    }

    @Override
    public HDF5TimeDuration getTimeDurationAttribute(String objectPath, String attributeName)
    {
        return dateTimeReader.getTimeDurationAttribute(objectPath, attributeName);
    }

    @Override
    public boolean isTimeDuration(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.isTimeDuration(objectPath);
    }

    @Override
    public boolean isTimeStamp(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.isTimeStamp(objectPath);
    }

    @Override
    public boolean isTimeDuration(String objectPath, String attributeName) throws HDF5JavaException
    {
        return dateTimeReader.isTimeDuration(objectPath, attributeName);
    }

    @Override
    public HDF5TimeUnit tryGetTimeUnit(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return dateTimeReader.tryGetTimeUnit(objectPath, attributeName);
    }

    @Override
    public long[] getTimeStampArrayAttribute(String objectPath, String attributeName)
    {
        return dateTimeReader.getTimeStampArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Date[] getDateArrayAttribute(String objectPath, String attributeName)
    {
        return dateTimeReader.getDateArrayAttribute(objectPath, attributeName);
    }

    @Override
    public HDF5TimeDurationArray getTimeDurationArrayAttribute(String objectPath,
            String attributeName)
    {
        return dateTimeReader.getTimeDurationArrayAttribute(objectPath, attributeName);
    }

    @Override
    public HDF5TimeUnit tryGetTimeUnit(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.tryGetTimeUnit(objectPath);
    }

    @Override
    @Deprecated
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(String dataSetPath,
            HDF5TimeUnit timeUnit) throws HDF5JavaException
    {
        return dateTimeReader.getTimeDurationArrayNaturalBlocks(dataSetPath, timeUnit);
    }

    @Override
    @Deprecated
    public Iterable<HDF5DataBlock<HDF5TimeDuration[]>> getTimeDurationAndUnitArrayNaturalBlocks(
            String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.getTimeDurationAndUnitArrayNaturalBlocks(objectPath);
    }

    @Override
    public Iterable<HDF5DataBlock<long[]>> getTimeStampArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return dateTimeReader.getTimeStampArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public Date readDate(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readDate(objectPath);
    }

    @Override
    public Date[] readDateArray(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readDateArray(objectPath);
    }

    @Override
    @Deprecated
    public long readTimeDuration(String objectPath, HDF5TimeUnit timeUnit) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDuration(objectPath, timeUnit);
    }

    @Override
    public HDF5TimeDuration readTimeDuration(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDuration(objectPath);
    }

    @Override
    @Deprecated
    public HDF5TimeDuration readTimeDurationAndUnit(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationAndUnit(objectPath);
    }

    @Override
    @Deprecated
    public long[] readTimeDurationArray(String objectPath, HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationArray(objectPath, timeUnit);
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArray(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationArray(objectPath);
    }

    @Override
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArray(String objectPath)
            throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationAndUnitArray(objectPath);
    }

    @Override
    @Deprecated
    public long[] readTimeDurationArrayBlock(String objectPath, int blockSize, long blockNumber,
            HDF5TimeUnit timeUnit)
    {
        return dateTimeReader.readTimeDurationArrayBlock(objectPath, blockSize, blockNumber,
                timeUnit);
    }

    @Override
    @Deprecated
    public long[] readTimeDurationArrayBlockWithOffset(String objectPath, int blockSize,
            long offset, HDF5TimeUnit timeUnit)
    {
        return dateTimeReader.readTimeDurationArrayBlockWithOffset(objectPath, blockSize, offset,
                timeUnit);
    }

    @Override
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlock(String objectPath, int blockSize,
            long blockNumber) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationAndUnitArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlockWithOffset(String objectPath,
            int blockSize, long offset) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationAndUnitArrayBlockWithOffset(objectPath, blockSize,
                offset);
    }

    @Override
    public long readTimeStamp(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readTimeStamp(objectPath);
    }

    @Override
    public long[] readTimeStampArray(String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.readTimeStampArray(objectPath);
    }

    @Override
    public long[] readTimeStampArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return dateTimeReader.readTimeStampArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public long[] readTimeStampArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return dateTimeReader.readTimeStampArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    //
    // Reference
    //

    @Override
    public String readObjectReference(final String objectPath)
    {
        return referenceReader.readObjectReference(objectPath);
    }

    @Override
    public String readObjectReference(String objectPath, boolean resolveName)
    {
        return referenceReader.readObjectReference(objectPath, resolveName);
    }

    @Override
    public String[] readObjectReferenceArrayBlock(String objectPath, int blockSize,
            long blockNumber, boolean resolveName)
    {
        return referenceReader.readObjectReferenceArrayBlock(objectPath, blockSize, blockNumber,
                resolveName);
    }

    @Override
    public String[] readObjectReferenceArrayBlockWithOffset(String objectPath, int blockSize,
            long offset, boolean resolveName)
    {
        return referenceReader.readObjectReferenceArrayBlockWithOffset(objectPath, blockSize,
                offset, resolveName);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArrayBlock(String objectPath,
            int[] blockDimensions, long[] blockNumber, boolean resolveName)
    {
        return referenceReader.readObjectReferenceMDArrayBlock(objectPath, blockDimensions,
                blockNumber, resolveName);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArrayBlockWithOffset(String objectPath,
            int[] blockDimensions, long[] offset, boolean resolveName)
    {
        return referenceReader.readObjectReferenceMDArrayBlockWithOffset(objectPath,
                blockDimensions, offset, resolveName);
    }

    @Override
    public Iterable<HDF5DataBlock<String[]>> getObjectReferenceArrayNaturalBlocks(
            String dataSetPath, boolean resolveName)
    {
        return referenceReader.getObjectReferenceArrayNaturalBlocks(dataSetPath, resolveName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getObjectReferenceMDArrayNaturalBlocks(
            String dataSetPath, boolean resolveName)
    {
        return referenceReader.getObjectReferenceMDArrayNaturalBlocks(dataSetPath, resolveName);
    }

    @Override
    public String[] readObjectReferenceArray(String objectPath)
    {
        return referenceReader.readObjectReferenceArray(objectPath);
    }

    @Override
    public String[] readObjectReferenceArray(String objectPath, boolean resolveName)
    {
        return referenceReader.readObjectReferenceArray(objectPath, resolveName);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArray(String objectPath)
    {
        return referenceReader.readObjectReferenceMDArray(objectPath);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArray(String objectPath, boolean resolveName)
    {
        return referenceReader.readObjectReferenceMDArray(objectPath, resolveName);
    }

    @Override
    public String getObjectReferenceAttribute(String objectPath, String attributeName,
            boolean resolveName)
    {
        return referenceReader.getObjectReferenceAttribute(objectPath, attributeName, resolveName);
    }

    @Override
    public String[] getObjectReferenceArrayAttribute(String objectPath, String attributeName,
            boolean resolveName)
    {
        return referenceReader.getObjectReferenceArrayAttribute(objectPath, attributeName,
                resolveName);
    }

    @Override
    public MDArray<String> getObjectReferenceMDArrayAttribute(String objectPath,
            String attributeName, boolean resolveName)
    {
        return referenceReader.getObjectReferenceMDArrayAttribute(objectPath, attributeName,
                resolveName);
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArrayBlock(String objectPath, int blockSize,
            long blockNumber) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public HDF5TimeDurationArray readTimeDurationArrayBlockWithOffset(String objectPath,
            int blockSize, long offset) throws HDF5JavaException
    {
        return dateTimeReader.readTimeDurationArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getTimeDurationArrayNaturalBlocks(
            String objectPath) throws HDF5JavaException
    {
        return dateTimeReader.getTimeDurationArrayNaturalBlocks(objectPath);
    }

    //
    // References
    //

    @Override
    public String resolvePath(String reference) throws HDF5JavaException
    {
        return referenceReader.resolvePath(reference);
    }

    @Override
    public String getObjectReferenceAttribute(final String objectPath, final String attributeName)
    {
        return referenceReader.getObjectReferenceAttribute(objectPath, attributeName);
    }

    @Override
    public String[] getObjectReferenceArrayAttribute(String objectPath, String attributeName)
    {
        return referenceReader.getObjectReferenceArrayAttribute(objectPath, attributeName);
    }

    @Override
    public MDArray<String> getObjectReferenceMDArrayAttribute(String objectPath,
            String attributeName)
    {
        return referenceReader.getObjectReferenceMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public String[] readObjectReferenceArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return referenceReader.readObjectReferenceArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public String[] readObjectReferenceArrayBlockWithOffset(String objectPath, int blockSize,
            long offset)
    {
        return referenceReader.readObjectReferenceArrayBlockWithOffset(objectPath, blockSize,
                offset);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArrayBlock(String objectPath,
            int[] blockDimensions, long[] blockNumber)
    {
        return referenceReader.readObjectReferenceMDArrayBlock(objectPath, blockDimensions,
                blockNumber);
    }

    @Override
    public MDArray<String> readObjectReferenceMDArrayBlockWithOffset(String objectPath,
            int[] blockDimensions, long[] offset)
    {
        return referenceReader.readObjectReferenceMDArrayBlockWithOffset(objectPath,
                blockDimensions, offset);
    }

    @Override
    public Iterable<HDF5DataBlock<String[]>> getObjectReferenceArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return referenceReader.getObjectReferenceArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getObjectReferenceMDArrayNaturalBlocks(
            String dataSetPath)
    {
        return referenceReader.getObjectReferenceMDArrayNaturalBlocks(dataSetPath);
    }

    //
    // String
    //

    @Override
    public IHDF5StringReader strings()
    {
        return stringReader;
    }

    @Override
    public String getStringAttribute(String objectPath, String attributeName)
    {
        return stringReader.getAttr(objectPath, attributeName);
    }

    @Override
    public String[] getStringArrayAttribute(String objectPath, String attributeName)
    {
        return stringReader.getArrayAttr(objectPath, attributeName);
    }

    @Override
    public MDArray<String> getStringMDArrayAttribute(String objectPath, String attributeName)
    {
        return stringReader.getMDArrayAttr(objectPath, attributeName);
    }

    @Override
    public String readString(String objectPath) throws HDF5JavaException
    {
        return stringReader.read(objectPath);
    }

    @Override
    public String[] readStringArray(String objectPath) throws HDF5JavaException
    {
        return stringReader.readArray(objectPath);
    }

    @Override
    public String[] readStringArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return stringReader.readArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public String[] readStringArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return stringReader.readArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDArray<String> readStringMDArray(String objectPath)
    {
        return stringReader.readMDArray(objectPath);
    }

    @Override
    public MDArray<String> readStringMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return stringReader.readMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDArray<String> readStringMDArrayBlockWithOffset(String objectPath,
            int[] blockDimensions, long[] offset)
    {
        return stringReader.readMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public Iterable<HDF5DataBlock<String[]>> getStringArrayNaturalBlocks(String objectPath)
            throws HDF5JavaException
    {
        return stringReader.getArrayNaturalBlocks(objectPath);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getStringMDArrayNaturalBlocks(
            String objectPath)
    {
        return stringReader.getMDArrayNaturalBlocks(objectPath);
    }

    //
    // Enums
    //

    @Override
    public IHDF5EnumReader enums()
    {
        return enumReader;
    }

    @Override
    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            String objectPath, HDF5EnumerationType enumType) throws HDF5JavaException
    {
        return enumReader.getArrayBlocks(objectPath, enumType);
    }

    @Override
    public Iterable<HDF5DataBlock<HDF5EnumerationValueArray>> getEnumArrayNaturalBlocks(
            String objectPath) throws HDF5JavaException
    {
        return enumReader.getArrayBlocks(objectPath);
    }

    @Override
    public HDF5EnumerationValue readEnum(String objectPath, HDF5EnumerationType enumType)
            throws HDF5JavaException
    {
        return enumReader.read(objectPath, enumType);
    }

    @Override
    public HDF5EnumerationValue readEnum(String objectPath) throws HDF5JavaException
    {
        return enumReader.read(objectPath);
    }

    @Override
    public <T extends Enum<T>> T readEnum(String objectPath, Class<T> enumClass)
            throws HDF5JavaException
    {
        return enumReader.read(objectPath, enumClass);
    }

    @Override
    public HDF5EnumerationValueArray readEnumArray(String objectPath, HDF5EnumerationType enumType)
            throws HDF5JavaException
    {
        return enumReader.readArray(objectPath, enumType);
    }

    @Override
    public HDF5EnumerationValueArray readEnumArray(String objectPath) throws HDF5JavaException
    {
        return enumReader.readArray(objectPath);
    }

    @Override
    public <T extends Enum<T>> T[] readEnumArray(String objectPath, Class<T> enumClass)
            throws HDF5JavaException
    {
        return readEnumArray(objectPath).toEnumArray(enumClass);
    }

    @Override
    public String[] readEnumArrayAsString(String objectPath) throws HDF5JavaException
    {
        return enumReader.readArray(objectPath).toStringArray();
    }

    @Override
    public HDF5EnumerationValueArray readEnumArrayBlock(String objectPath,
            HDF5EnumerationType enumType, int blockSize, long blockNumber)
    {
        return enumReader.readArrayBlock(objectPath, enumType, blockSize, blockNumber);
    }

    @Override
    public HDF5EnumerationValueArray readEnumArrayBlock(String objectPath, int blockSize,
            long blockNumber)
    {
        return enumReader.readArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(String objectPath,
            HDF5EnumerationType enumType, int blockSize, long offset)
    {
        return enumReader.readArrayBlockWithOffset(objectPath, enumType, blockSize, offset);
    }

    @Override
    public HDF5EnumerationValueArray readEnumArrayBlockWithOffset(String objectPath, int blockSize,
            long offset)
    {
        return enumReader.readArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public String readEnumAsString(String objectPath) throws HDF5JavaException
    {
        return enumReader.readAsString(objectPath);
    }

    //
    // Compounds
    //

    @Override
    public IHDF5CompoundReader compounds()
    {
        return compoundReader;
    }

    @Override
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            HDF5CompoundType<T> type, IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        return compoundReader.getArrayBlocks(objectPath, type, inspectorOrNull);
    }

    @Override
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return compoundReader.getArrayBlocks(objectPath, type);
    }

    @Override
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, HDF5CompoundType<T> type, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        return compoundReader.getMDArrayBlocks(objectPath, type, inspectorOrNull);
    }

    @Override
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return compoundReader.getMDArrayBlocks(objectPath, type);
    }

    @Override
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(String objectPath,
            Class<T> pojoClass) throws HDF5JavaException
    {
        return compoundReader.getArrayBlocks(objectPath, pojoClass);
    }

    @Override
    public <T> Iterable<HDF5MDDataBlock<MDArray<T>>> getCompoundMDArrayNaturalBlocks(
            String objectPath, Class<T> pojoClass) throws HDF5JavaException
    {
        return compoundReader.getMDArrayBlocks(objectPath, pojoClass);
    }

    @Override
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(String dataSetPath,
            boolean sortAlphabetically) throws HDF5JavaException
    {
        final HDF5CompoundMemberInformation[] compoundInformation =
                compoundReader.getDataSetInfo(dataSetPath, DataTypeInfoOptions.DEFAULT);
        if (sortAlphabetically)
        {
            Arrays.sort(compoundInformation);
        }
        return compoundInformation;
    }

    @Override
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(String dataSetPath)
            throws HDF5JavaException
    {
        return compoundReader.getDataSetInfo(dataSetPath);
    }

    @Override
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(Class<T> compoundClass)
    {
        return compoundReader.getMemberInfo(compoundClass);
    }

    @Override
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(String dataTypeName)
    {
        return compoundReader.getMemberInfo(dataTypeName);
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(Class<T> pojoClass,
            HDF5CompoundMemberMapping... members)
    {
        return compoundReader.getType(pojoClass, members);
    }

    @Override
    public <T> HDF5CompoundType<T> getCompoundType(String name, Class<T> compoundType,
            HDF5CompoundMemberMapping... members)
    {
        return compoundReader.getType(name, compoundType, members);
    }

    @Override
    public <T> HDF5CompoundType<T> getDataSetCompoundType(String objectPath, Class<T> compoundClass)
    {
        return compoundReader.getDataSetType(objectPath, compoundClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getAttributeCompoundType(String objectPath,
            String attributeName, Class<T> pojoClass)
    {
        return compoundReader.getAttributeType(objectPath, attributeName, pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(Class<T> pojoClass)
    {
        return compoundReader.getInferredType(pojoClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(String name, Class<T> compoundType)
    {
        return compoundReader.getInferredType(name, compoundType);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(String name, T template)
    {
        return compoundReader.getInferredType(name, template);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(T template)
    {
        return compoundReader.getInferredType(template);
    }

    @Override
    public <T> HDF5CompoundType<T> getInferredCompoundType(String name, T template,
            HDF5CompoundMappingHints hints)
    {
        return compoundReader.getInferredType(name, template, hints);
    }

    @Override
    public HDF5CompoundType<List<?>> getInferredCompoundType(String name, List<String> memberNames,
            List<?> template)
    {
        return compoundReader.getInferredType(name, memberNames, template);
    }

    @Override
    public HDF5CompoundType<Object[]> getInferredCompoundType(String name, String[] memberNames,
            Object[] template)
    {
        return compoundReader.getInferredType(name, memberNames, template);
    }

    @Override
    public HDF5CompoundType<List<?>> getInferredCompoundType(List<String> memberNames,
            List<?> template)
    {
        return compoundReader.getInferredType(memberNames, template);
    }

    @Override
    public HDF5CompoundType<Object[]> getInferredCompoundType(String[] memberNames,
            Object[] template)
    {
        return compoundReader.getInferredType(memberNames, template);
    }

    @Override
    public <T> HDF5CompoundType<T> getNamedCompoundType(Class<T> compoundClass)
    {
        return compoundReader.getNamedType(compoundClass);
    }

    @Override
    public <T> HDF5CompoundType<T> getNamedCompoundType(String dataTypeName, Class<T> compoundClass)
    {
        return compoundReader.getNamedType(dataTypeName, compoundClass);
    }

    @Override
    public <T> T readCompound(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        return compoundReader.read(objectPath, type, inspectorOrNull);
    }

    @Override
    public <T> T readCompound(String objectPath, HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return compoundReader.read(objectPath, type);
    }

    @Override
    public <T> T readCompound(String objectPath, Class<T> pojoClass) throws HDF5JavaException
    {
        return compoundReader.read(objectPath, pojoClass);
    }

    @Override
    public <T> T[] readCompoundArray(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        return compoundReader.readArray(objectPath, type, inspectorOrNull);
    }

    @Override
    public <T> T[] readCompoundArray(String objectPath, HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return compoundReader.readArray(objectPath, type);
    }

    @Override
    public <T> T[] readCompoundArray(String objectPath, Class<T> pojoClass)
            throws HDF5JavaException
    {
        return compoundReader.readArray(objectPath, pojoClass);
    }

    @Override
    public <T> T[] readCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long blockNumber, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        return compoundReader.readArrayBlock(objectPath, type, blockSize, blockNumber,
                inspectorOrNull);
    }

    @Override
    public <T> T[] readCompoundArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long blockNumber) throws HDF5JavaException
    {
        return compoundReader.readArrayBlock(objectPath, type, blockSize, blockNumber);
    }

    @Override
    public <T> T[] readCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long offset, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        return compoundReader.readArrayBlockWithOffset(objectPath, type, blockSize, offset,
                inspectorOrNull);
    }

    @Override
    public <T> T[] readCompoundArrayBlockWithOffset(String objectPath, HDF5CompoundType<T> type,
            int blockSize, long offset) throws HDF5JavaException
    {
        return compoundReader.readArrayBlockWithOffset(objectPath, type, blockSize, offset);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArray(String objectPath, HDF5CompoundType<T> type,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        return compoundReader.readMDArray(objectPath, type, inspectorOrNull);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArray(String objectPath, HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return compoundReader.readMDArray(objectPath, type);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArray(String objectPath, Class<T> pojoClass)
            throws HDF5JavaException
    {
        return compoundReader.readMDArray(objectPath, pojoClass);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int[] blockDimensions, long[] blockNumber, IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        return compoundReader.readMDArrayBlock(objectPath, type, blockDimensions, blockNumber,
                inspectorOrNull);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArrayBlock(String objectPath, HDF5CompoundType<T> type,
            int[] blockDimensions, long[] blockNumber) throws HDF5JavaException
    {
        return compoundReader.readMDArrayBlock(objectPath, type, blockDimensions, blockNumber);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, int[] blockDimensions, long[] offset,
            IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        return compoundReader.readMDArrayBlockWithOffset(objectPath, type, blockDimensions, offset,
                inspectorOrNull);
    }

    @Override
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(String objectPath,
            HDF5CompoundType<T> type, int[] blockDimensions, long[] offset)
            throws HDF5JavaException
    {
        return compoundReader.readMDArrayBlockWithOffset(objectPath, type, blockDimensions, offset);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - START
    // ------------------------------------------------------------------------------

    @Override
    public byte[] getByteArrayAttribute(String objectPath, String attributeName)
    {
        return byteReader.getByteArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<byte[]>> getByteArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return byteReader.getByteArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public byte getByteAttribute(String objectPath, String attributeName)
    {
        return byteReader.getByteAttribute(objectPath, attributeName);
    }

    @Override
    public MDByteArray getByteMDArrayAttribute(String objectPath, String attributeName)
    {
        return byteReader.getByteMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDByteArray>> getByteMDArrayNaturalBlocks(String dataSetPath)
    {
        return byteReader.getByteMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public byte[][] getByteMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return byteReader.getByteMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public byte readByte(String objectPath)
    {
        return byteReader.readByte(objectPath);
    }

    @Override
    public byte[] readByteArray(String objectPath)
    {
        return byteReader.readByteArray(objectPath);
    }

    @Override
    public byte[] readByteArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return byteReader.readByteArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public byte[] readByteArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return byteReader.readByteArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDByteArray readByteMDArray(String objectPath)
    {
        return byteReader.readByteMDArray(objectPath);
    }

    @Override
    public MDByteArray readByteMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return byteReader.readByteMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDByteArray readByteMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return byteReader.readByteMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public byte[][] readByteMatrix(String objectPath) throws HDF5JavaException
    {
        return byteReader.readByteMatrix(objectPath);
    }

    @Override
    public byte[][] readByteMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return byteReader.readByteMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public byte[][] readByteMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return byteReader.readByteMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    @Override
    public int[] readToByteMDArrayBlockWithOffset(String objectPath, MDByteArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return byteReader.readToByteMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToByteMDArrayWithOffset(String objectPath, MDByteArray array,
            int[] memoryOffset)
    {
        return byteReader.readToByteMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    @Override
    public double[] getDoubleArrayAttribute(String objectPath, String attributeName)
    {
        return doubleReader.getDoubleArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<double[]>> getDoubleArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return doubleReader.getDoubleArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public double getDoubleAttribute(String objectPath, String attributeName)
    {
        return doubleReader.getDoubleAttribute(objectPath, attributeName);
    }

    @Override
    public MDDoubleArray getDoubleMDArrayAttribute(String objectPath, String attributeName)
    {
        return doubleReader.getDoubleMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDDoubleArray>> getDoubleMDArrayNaturalBlocks(String dataSetPath)
    {
        return doubleReader.getDoubleMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public double[][] getDoubleMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return doubleReader.getDoubleMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public double readDouble(String objectPath)
    {
        return doubleReader.readDouble(objectPath);
    }

    @Override
    public double[] readDoubleArray(String objectPath)
    {
        return doubleReader.readDoubleArray(objectPath);
    }

    @Override
    public double[] readDoubleArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return doubleReader.readDoubleArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public double[] readDoubleArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return doubleReader.readDoubleArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDDoubleArray readDoubleMDArray(String objectPath)
    {
        return doubleReader.readDoubleMDArray(objectPath);
    }

    @Override
    public MDDoubleArray readDoubleMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return doubleReader.readDoubleMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDDoubleArray readDoubleMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return doubleReader.readDoubleMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public double[][] readDoubleMatrix(String objectPath) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrix(objectPath);
    }

    @Override
    public double[][] readDoubleMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public double[][] readDoubleMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    @Override
    public int[] readToDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return doubleReader.readToDoubleMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToDoubleMDArrayWithOffset(String objectPath, MDDoubleArray array,
            int[] memoryOffset)
    {
        return doubleReader.readToDoubleMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    @Override
    public float[] getFloatArrayAttribute(String objectPath, String attributeName)
    {
        return floatReader.getFloatArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<float[]>> getFloatArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return floatReader.getFloatArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public float getFloatAttribute(String objectPath, String attributeName)
    {
        return floatReader.getFloatAttribute(objectPath, attributeName);
    }

    @Override
    public MDFloatArray getFloatMDArrayAttribute(String objectPath, String attributeName)
    {
        return floatReader.getFloatMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDFloatArray>> getFloatMDArrayNaturalBlocks(String dataSetPath)
    {
        return floatReader.getFloatMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public float[][] getFloatMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return floatReader.getFloatMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public float readFloat(String objectPath)
    {
        return floatReader.readFloat(objectPath);
    }

    @Override
    public float[] readFloatArray(String objectPath)
    {
        return floatReader.readFloatArray(objectPath);
    }

    @Override
    public float[] readFloatArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return floatReader.readFloatArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public float[] readFloatArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return floatReader.readFloatArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDFloatArray readFloatMDArray(String objectPath)
    {
        return floatReader.readFloatMDArray(objectPath);
    }

    @Override
    public MDFloatArray readFloatMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return floatReader.readFloatMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDFloatArray readFloatMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return floatReader.readFloatMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public float[][] readFloatMatrix(String objectPath) throws HDF5JavaException
    {
        return floatReader.readFloatMatrix(objectPath);
    }

    @Override
    public float[][] readFloatMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return floatReader.readFloatMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public float[][] readFloatMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return floatReader.readFloatMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    @Override
    public int[] readToFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return floatReader.readToFloatMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToFloatMDArrayWithOffset(String objectPath, MDFloatArray array,
            int[] memoryOffset)
    {
        return floatReader.readToFloatMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    @Override
    public int[] getIntArrayAttribute(String objectPath, String attributeName)
    {
        return intReader.getIntArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return intReader.getIntArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public int getIntAttribute(String objectPath, String attributeName)
    {
        return intReader.getIntAttribute(objectPath, attributeName);
    }

    @Override
    public MDIntArray getIntMDArrayAttribute(String objectPath, String attributeName)
    {
        return intReader.getIntMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(String dataSetPath)
    {
        return intReader.getIntMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public int[][] getIntMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return intReader.getIntMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public int readInt(String objectPath)
    {
        return intReader.readInt(objectPath);
    }

    @Override
    public int[] readIntArray(String objectPath)
    {
        return intReader.readIntArray(objectPath);
    }

    @Override
    public int[] readIntArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return intReader.readIntArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public int[] readIntArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return intReader.readIntArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDIntArray readIntMDArray(String objectPath)
    {
        return intReader.readIntMDArray(objectPath);
    }

    @Override
    public MDIntArray readIntMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return intReader.readIntMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDIntArray readIntMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return intReader.readIntMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public int[][] readIntMatrix(String objectPath) throws HDF5JavaException
    {
        return intReader.readIntMatrix(objectPath);
    }

    @Override
    public int[][] readIntMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return intReader.readIntMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public int[][] readIntMatrixBlockWithOffset(String objectPath, int blockSizeX, int blockSizeY,
            long offsetX, long offsetY) throws HDF5JavaException
    {
        return intReader.readIntMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY, offsetX,
                offsetY);
    }

    @Override
    public int[] readToIntMDArrayBlockWithOffset(String objectPath, MDIntArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return intReader.readToIntMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToIntMDArrayWithOffset(String objectPath, MDIntArray array, int[] memoryOffset)
    {
        return intReader.readToIntMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    @Override
    public long[] getLongArrayAttribute(String objectPath, String attributeName)
    {
        return longReader.getLongArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<long[]>> getLongArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return longReader.getLongArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public long getLongAttribute(String objectPath, String attributeName)
    {
        return longReader.getLongAttribute(objectPath, attributeName);
    }

    @Override
    public MDLongArray getLongMDArrayAttribute(String objectPath, String attributeName)
    {
        return longReader.getLongMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDLongArray>> getLongMDArrayNaturalBlocks(String dataSetPath)
    {
        return longReader.getLongMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public long[][] getLongMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return longReader.getLongMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public long readLong(String objectPath)
    {
        return longReader.readLong(objectPath);
    }

    @Override
    public long[] readLongArray(String objectPath)
    {
        return longReader.readLongArray(objectPath);
    }

    @Override
    public long[] readLongArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return longReader.readLongArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public long[] readLongArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return longReader.readLongArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDLongArray readLongMDArray(String objectPath)
    {
        return longReader.readLongMDArray(objectPath);
    }

    @Override
    public MDLongArray readLongMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return longReader.readLongMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDLongArray readLongMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return longReader.readLongMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public long[][] readLongMatrix(String objectPath) throws HDF5JavaException
    {
        return longReader.readLongMatrix(objectPath);
    }

    @Override
    public long[][] readLongMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return longReader.readLongMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public long[][] readLongMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return longReader.readLongMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    @Override
    public int[] readToLongMDArrayBlockWithOffset(String objectPath, MDLongArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return longReader.readToLongMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToLongMDArrayWithOffset(String objectPath, MDLongArray array,
            int[] memoryOffset)
    {
        return longReader.readToLongMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    @Override
    public short[] getShortArrayAttribute(String objectPath, String attributeName)
    {
        return shortReader.getShortArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5DataBlock<short[]>> getShortArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return shortReader.getShortArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public short getShortAttribute(String objectPath, String attributeName)
    {
        return shortReader.getShortAttribute(objectPath, attributeName);
    }

    @Override
    public MDShortArray getShortMDArrayAttribute(String objectPath, String attributeName)
    {
        return shortReader.getShortMDArrayAttribute(objectPath, attributeName);
    }

    @Override
    public Iterable<HDF5MDDataBlock<MDShortArray>> getShortMDArrayNaturalBlocks(String dataSetPath)
    {
        return shortReader.getShortMDArrayNaturalBlocks(dataSetPath);
    }

    @Override
    public short[][] getShortMatrixAttribute(String objectPath, String attributeName)
            throws HDF5JavaException
    {
        return shortReader.getShortMatrixAttribute(objectPath, attributeName);
    }

    @Override
    public short readShort(String objectPath)
    {
        return shortReader.readShort(objectPath);
    }

    @Override
    public short[] readShortArray(String objectPath)
    {
        return shortReader.readShortArray(objectPath);
    }

    @Override
    public short[] readShortArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return shortReader.readShortArrayBlock(objectPath, blockSize, blockNumber);
    }

    @Override
    public short[] readShortArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return shortReader.readShortArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    @Override
    public MDShortArray readShortMDArray(String objectPath)
    {
        return shortReader.readShortMDArray(objectPath);
    }

    @Override
    public MDShortArray readShortMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return shortReader.readShortMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    @Override
    public MDShortArray readShortMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return shortReader.readShortMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    @Override
    public short[][] readShortMatrix(String objectPath) throws HDF5JavaException
    {
        return shortReader.readShortMatrix(objectPath);
    }

    @Override
    public short[][] readShortMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return shortReader.readShortMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    @Override
    public short[][] readShortMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return shortReader.readShortMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    @Override
    public int[] readToShortMDArrayBlockWithOffset(String objectPath, MDShortArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        return shortReader.readToShortMDArrayBlockWithOffset(objectPath, array, blockDimensions,
                offset, memoryOffset);
    }

    @Override
    public int[] readToShortMDArrayWithOffset(String objectPath, MDShortArray array,
            int[] memoryOffset)
    {
        return shortReader.readToShortMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - END
    // ------------------------------------------------------------------------------

}
