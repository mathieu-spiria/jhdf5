/*
 * Copyright 2009 ETH Zuerich, CISD
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

import static ch.systemsx.cisd.hdf5.HDF5Utils.BOOLEAN_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_ATTRIBUTE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_MEMBERS_ATTRIBUTE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.getOneDimensionalArraySize;
import static ch.systemsx.cisd.hdf5.HDF5Utils.removeInternalNames;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5S_ALL;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_ENUM;
import static ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants.H5T_STRING;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.cleanup.CleanUpCallable;
import ch.systemsx.cisd.hdf5.cleanup.CleanUpRegistry;
import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants;

/**
 * Class that provides base methods for reading HDF5 files.
 * 
 * @author Bernd Rinn
 */
class HDF5BaseReader
{

    /** State that this reader / writer is currently in. */
    protected enum State
    {
        CONFIG, OPEN, CLOSED
    }

    /** The size of a reference in bytes. */
    static final int REFERENCE_SIZE_IN_BYTES = 8;

    protected final File hdf5File;

    protected final CleanUpCallable runner;

    protected final CleanUpRegistry fileRegistry;

    protected final boolean performNumericConversions;

    /** Map from named data types to ids. */
    private final Map<String, Integer> namedDataTypeMap;

    private class DataTypeContainer
    {
        final int typeId;

        final String typePath;

        DataTypeContainer(int typeId, String typePath)
        {
            this.typeId = typeId;
            this.typePath = typePath;
        }
    }

    private final List<DataTypeContainer> namedDataTypeList;

    protected final HDF5 h5;

    protected final int fileId;

    protected final int booleanDataTypeId;

    protected final HDF5EnumerationType typeVariantDataType;

    protected State state;

    final CharacterEncoding encoding;

    HDF5BaseReader(File hdf5File, boolean performNumericConversions, boolean useUTF8CharEncoding,
            FileFormat fileFormat, boolean overwrite)
    {
        assert hdf5File != null;

        this.performNumericConversions = performNumericConversions;
        this.encoding = useUTF8CharEncoding ? CharacterEncoding.UTF8 : CharacterEncoding.ASCII;
        this.hdf5File = hdf5File.getAbsoluteFile();
        this.runner = new CleanUpCallable();
        this.fileRegistry = new CleanUpRegistry();
        this.namedDataTypeMap = new HashMap<String, Integer>();
        this.namedDataTypeList = new ArrayList<DataTypeContainer>();
        h5 = new HDF5(fileRegistry, performNumericConversions, useUTF8CharEncoding);
        fileId = openFile(fileFormat, overwrite);
        state = State.OPEN;
        readNamedDataTypes();
        booleanDataTypeId = openOrCreateBooleanDataType();
        typeVariantDataType = openOrCreateTypeVariantDataType();
    }

    void copyObject(String srcPath, int dstFileId, String dstPath)
    {
        final boolean dstIsDir = dstPath.endsWith("/");
        if (dstIsDir && h5.exists(dstFileId, dstPath) == false)
        {
            h5.createGroup(dstFileId, dstPath);
        }
        if ("/".equals(srcPath))
        {
            final String dstDir = dstIsDir ? dstPath : dstPath + "/";
            for (String object : getGroupMembers("/"))
            {
                h5.copyObject(fileId, object, dstFileId, dstDir + object);
            }
        } else if (dstIsDir)
        {
            final int idx = srcPath.lastIndexOf('/');
            final String sourceObjectName = srcPath.substring(idx < 0 ? 0 : idx);
            h5.copyObject(fileId, srcPath, dstFileId, dstPath + sourceObjectName);
        } else
        {
            h5.copyObject(fileId, srcPath, dstFileId, dstPath);
        }
    }

    int openFile(FileFormat fileFormat, boolean overwrite)
    {
        if (hdf5File.exists() == false)
        {
            throw new HDF5JavaException("File " + this.hdf5File.getPath() + " does not exit.");
        }
        if (hdf5File.canRead() == false)
        {
            throw new HDF5JavaException("File " + this.hdf5File.getPath() + " not readable.");
        }
        return h5.openFileReadOnly(hdf5File.getPath(), fileRegistry);
    }

    void checkOpen() throws HDF5JavaException
    {
        if (state != State.OPEN)
        {
            final String msg =
                    "HDF5 file '" + hdf5File.getPath() + "' is "
                            + (state == State.CLOSED ? "closed." : "not opened yet.");
            throw new HDF5JavaException(msg);
        }
    }

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    synchronized void close()
    {
        if (state == State.OPEN)
        {
            fileRegistry.cleanUp(false);
        }
        state = State.CLOSED;
    }

    int openOrCreateBooleanDataType()
    {
        int dataTypeId = getDataTypeId(BOOLEAN_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = createBooleanDataType();
            commitDataType(BOOLEAN_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    String tryGetDataTypePath(int dataTypeId)
    {
        for (DataTypeContainer namedDataType : namedDataTypeList)
        {
            if (h5.dataTypesAreEqual(dataTypeId, namedDataType.typeId))
            {
                return namedDataType.typePath;
            }
        }
        return h5.tryGetDataTypePath(dataTypeId);
    }

    void renameNamedDataType(String oldPath, String newPath)
    {
        final Integer typeIdOrNull = namedDataTypeMap.get(oldPath);
        if (typeIdOrNull != null)
        {
            namedDataTypeMap.put(newPath, typeIdOrNull);
        }
        for (int i = 0; i < namedDataTypeList.size(); ++i)
        {
            final DataTypeContainer c = namedDataTypeList.get(i); 
            if (c.typePath.equals(oldPath))
            {
                namedDataTypeList.set(i, new DataTypeContainer(c.typeId, newPath));
            }
        }
    }

    String tryGetDataTypeName(int dataTypeId, HDF5DataClass dataClass)
    {
        final String dataTypePathOrNull = tryGetDataTypePath(dataTypeId);
        return HDF5Utils.tryGetDataTypeNameFromPath(dataTypePathOrNull, dataClass);
    }

    int getDataTypeId(final String dataTypePath)
    {
        final Integer dataTypeIdOrNull = namedDataTypeMap.get(dataTypePath);
        if (dataTypeIdOrNull == null)
        {
            // Just in case of data types added to other groups than HDF5Utils.DATATYPE_GROUP
            if (h5.exists(fileId, dataTypePath))
            {
                final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
                namedDataTypeMap.put(dataTypePath, dataTypeId);
                return dataTypeId;
            } else
            {
                return -1;
            }
        } else
        {
            return dataTypeIdOrNull;
        }
    }

    int createBooleanDataType()
    {
        return h5.createDataTypeEnum(new String[]
            { "FALSE", "TRUE" }, fileRegistry);
    }

    HDF5EnumerationType openOrCreateTypeVariantDataType()
    {
        int dataTypeId = getDataTypeId(TYPE_VARIANT_DATA_TYPE);
        if (dataTypeId < 0)
        {
            return createTypeVariantDataType();
        }
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    HDF5EnumerationType createTypeVariantDataType()
    {
        final HDF5DataTypeVariant[] typeVariants = HDF5DataTypeVariant.values();
        final String[] typeVariantNames = new String[typeVariants.length];
        for (int i = 0; i < typeVariants.length; ++i)
        {
            typeVariantNames[i] = typeVariants[i].name();
        }
        final int dataTypeId = h5.createDataTypeEnum(typeVariantNames, fileRegistry);
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    void readNamedDataTypes()
    {
        if (h5.exists(fileId, DATATYPE_GROUP) == false)
        {
            return;
        }
        for (String dataTypePath : getGroupMemberPaths(DATATYPE_GROUP))
        {
            final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
            namedDataTypeMap.put(dataTypePath, dataTypeId);
            namedDataTypeList.add(new DataTypeContainer(dataTypeId, dataTypePath));
        }
    }

    void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        // Overwrite this method in writer.
    }

    /**
     * Class to store the parameters of a 1d data space.
     */
    static class DataSpaceParameters
    {
        final int memorySpaceId;

        final int dataSpaceId;

        final int blockSize;

        final long[] dimensions;

        DataSpaceParameters(int memorySpaceId, int dataSpaceId, int blockSize, long[] dimensions)
        {
            this.memorySpaceId = memorySpaceId;
            this.dataSpaceId = dataSpaceId;
            this.blockSize = blockSize;
            this.dimensions = dimensions;
        }
    }

    /**
     * Returns the {@link DataSpaceParameters} for the given <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, ICleanUpRegistry registry)
    {
        long[] dimensions = h5.getDataDimensions(dataSetId, registry);
        // Ensure backward compatibility with 8.10
        if (HDF5Utils.mightBeEmptyInStorage(dimensions)
                && h5.existsAttribute(dataSetId, HDF5Utils.DATASET_IS_EMPTY_LEGACY_ATTRIBUTE))
        {
            dimensions = new long[dimensions.length];
        }
        return new DataSpaceParameters(H5S_ALL, H5S_ALL, MDArray.getLength(dimensions), dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a 1d block of the given <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, final long offset,
            final int blockSize, ICleanUpRegistry registry)
    {
        return getSpaceParameters(dataSetId, 0, offset, blockSize, registry);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a 1d block of the given <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, final long memoryOffset,
            final long offset, final int blockSize, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final int effectiveBlockSize;
        final long[] dimensions;
        if (blockSize > 0)
        {
            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            dimensions = h5.getDataSpaceDimensions(dataSpaceId);
            if (dimensions.length != 1)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                        + dimensions.length + ")");
            }
            final long size = dimensions[0];
            final long maxFileBlockSize = size - offset;
            if (maxFileBlockSize <= 0)
            {
                throw new HDF5JavaException("Offset " + offset + " >= Size " + size);
            }
            final long maxMemoryBlockSize = size - memoryOffset;
            if (maxMemoryBlockSize <= 0)
            {
                throw new HDF5JavaException("Memory offset " + memoryOffset + " >= Size " + size);
            }
            effectiveBlockSize =
                    (int) Math.min(blockSize, Math.min(maxMemoryBlockSize, maxFileBlockSize));
            final long[] blockShape = new long[]
                { effectiveBlockSize };
            h5.setHyperslabBlock(dataSpaceId, new long[]
                { offset }, blockShape);
            memorySpaceId = h5.createSimpleDataSpace(blockShape, registry);
            h5.setHyperslabBlock(memorySpaceId, new long[]
                { memoryOffset }, blockShape);
        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            dimensions = h5.getDataDimensions(dataSetId);
            effectiveBlockSize = getOneDimensionalArraySize(dimensions);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, effectiveBlockSize, dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a multi-dimensional block of the given
     * <var>dataSetId</var>.
     */
    DataSpaceParameters getSpaceParameters(final int dataSetId, final long[] offset,
            final int[] blockDimensionsOrNull, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final long[] effectiveBlockDimensions;
        if (blockDimensionsOrNull != null)
        {
            assert offset != null;
            assert blockDimensionsOrNull.length == offset.length;

            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            final long[] dimensions = h5.getDataSpaceDimensions(dataSpaceId);
            if (dimensions.length != blockDimensionsOrNull.length)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank "
                        + blockDimensionsOrNull.length + " (rank=" + dimensions.length + ")");
            }
            effectiveBlockDimensions = new long[blockDimensionsOrNull.length];
            for (int i = 0; i < offset.length; ++i)
            {
                final long maxBlockSize = dimensions[i] - offset[i];
                if (maxBlockSize <= 0)
                {
                    throw new HDF5JavaException("Offset " + offset[i] + " >= Size " + dimensions[i]);
                }
                effectiveBlockDimensions[i] = Math.min(blockDimensionsOrNull[i], maxBlockSize);
            }
            h5.setHyperslabBlock(dataSpaceId, offset, effectiveBlockDimensions);
            memorySpaceId = h5.createSimpleDataSpace(effectiveBlockDimensions, registry);
        } else
        {
            memorySpaceId = H5S_ALL;
            dataSpaceId = H5S_ALL;
            effectiveBlockDimensions = h5.getDataDimensions(dataSetId);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId,
                MDArray.getLength(effectiveBlockDimensions), effectiveBlockDimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for the given <var>dataSetId</var> when they are
     * mapped to a block in memory.
     */
    DataSpaceParameters getBlockSpaceParameters(final int dataSetId, final int[] memoryOffset,
            final int[] memoryDimensions, ICleanUpRegistry registry)
    {
        assert memoryOffset != null;
        assert memoryDimensions != null;
        assert memoryDimensions.length == memoryOffset.length;

        final long[] dimensions = h5.getDataDimensions(dataSetId);
        final int memorySpaceId =
                h5.createSimpleDataSpace(MDArray.toLong(memoryDimensions), registry);
        for (int i = 0; i < dimensions.length; ++i)
        {
            if (dimensions[i] + memoryOffset[i] > memoryDimensions[i])
            {
                throw new HDF5JavaException("Dimensions " + dimensions[i] + " + memory offset "
                        + memoryOffset[i] + " >= memory buffer " + memoryDimensions[i]);
            }
        }
        h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset), dimensions);
        return new DataSpaceParameters(memorySpaceId, H5S_ALL, MDArray.getLength(dimensions),
                dimensions);
    }

    /**
     * Returns the {@link DataSpaceParameters} for a block of the given <var>dataSetId</var> when
     * they are mapped to a block in memory.
     */
    DataSpaceParameters getBlockSpaceParameters(final int dataSetId, final int[] memoryOffset,
            final int[] memoryDimensions, final long[] offset, final int[] blockDimensions,
            ICleanUpRegistry registry)
    {
        assert memoryOffset != null;
        assert memoryDimensions != null;
        assert offset != null;
        assert blockDimensions != null;
        assert memoryOffset.length == offset.length;
        assert memoryDimensions.length == memoryOffset.length;
        assert blockDimensions.length == offset.length;

        final int memorySpaceId;
        final int dataSpaceId;
        final long[] effectiveBlockDimensions;

        dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
        final long[] dimensions = h5.getDataSpaceDimensions(dataSpaceId);
        if (dimensions.length != blockDimensions.length)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank "
                    + blockDimensions.length + " (rank=" + dimensions.length + ")");
        }
        effectiveBlockDimensions = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            final long maxFileBlockSize = dimensions[i] - offset[i];
            if (maxFileBlockSize <= 0)
            {
                throw new HDF5JavaException("Offset " + offset[i] + " >= Size " + dimensions[i]);
            }
            final long maxMemoryBlockSize = memoryDimensions[i] - memoryOffset[i];
            if (maxMemoryBlockSize <= 0)
            {
                throw new HDF5JavaException("Memory offset " + memoryOffset[i] + " >= Size "
                        + memoryDimensions[i]);
            }
            effectiveBlockDimensions[i] =
                    Math.min(blockDimensions[i], Math.min(maxMemoryBlockSize, maxFileBlockSize));
        }
        h5.setHyperslabBlock(dataSpaceId, offset, effectiveBlockDimensions);
        memorySpaceId = h5.createSimpleDataSpace(MDArray.toLong(memoryDimensions), registry);
        h5.setHyperslabBlock(memorySpaceId, MDArray.toLong(memoryOffset), effectiveBlockDimensions);
        return new DataSpaceParameters(memorySpaceId, dataSpaceId,
                MDArray.getLength(effectiveBlockDimensions), effectiveBlockDimensions);
    }

    /**
     * Returns the native data type for the given <var>dataSetId</var>, or
     * <var>overrideDataTypeId</var>, if it is not negative.
     */
    int getNativeDataTypeId(final int dataSetId, final int overrideDataTypeId,
            ICleanUpRegistry registry)
    {
        final int nativeDataTypeId;
        if (overrideDataTypeId < 0)
        {
            nativeDataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
        } else
        {
            nativeDataTypeId = overrideDataTypeId;
        }
        return nativeDataTypeId;
    }

    /**
     * Returns the members of <var>groupPath</var>. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    List<String> getGroupMembers(final String groupPath)
    {
        assert groupPath != null;
        return removeInternalNames(getAllGroupMembers(groupPath));
    }

    /**
     * Returns all members of <var>groupPath</var>, including internal groups that may be used by
     * the library to do house-keeping. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    List<String> getAllGroupMembers(final String groupPath)
    {
        final String[] groupMemberArray = h5.getGroupMembers(fileId, groupPath);
        return new LinkedList<String>(Arrays.asList(groupMemberArray));
    }

    /**
     * Returns the paths of the members of <var>groupPath</var> (including the parent). The order is
     * <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the member paths for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    List<String> getGroupMemberPaths(final String groupPath)
    {
        final String superGroupName = (groupPath.equals("/") ? "/" : groupPath + "/");
        final List<String> memberNames = getGroupMembers(groupPath);
        for (int i = 0; i < memberNames.size(); ++i)
        {
            memberNames.set(i, superGroupName + memberNames.get(i));
        }
        return memberNames;
    }

    /**
     * Returns the information about a data set as a {@link HDF5DataTypeInformation} object. It is a
     * failure condition if the <var>dataSetPath</var> does not exist or does not identify a data
     * set.
     * 
     * @param dataSetPath The name (including path information) of the data set to return
     *            information about.
     */
    HDF5DataSetInformation getDataSetInformation(final String dataSetPath)
    {
        assert dataSetPath != null;

        final ICallableWithCleanUp<HDF5DataSetInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataSetInformation>()
                    {
                        public HDF5DataSetInformation call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, dataSetPath, registry);
                            final int dataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                            final HDF5DataTypeInformation dataTypeInfo =
                                    getDataTypeInformation(dataTypeId, registry);
                            final HDF5DataSetInformation dataSetInfo =
                                    new HDF5DataSetInformation(dataTypeInfo, tryGetTypeVariant(
                                            dataSetId, registry));
                            // Is it a variable-length string?
                            final boolean vlString =
                                    (dataTypeInfo.getDataClass() == HDF5DataClass.STRING && h5
                                            .isVariableLengthString(dataTypeId));
                            if (vlString)
                            {
                                dataTypeInfo.setElementSize(-1);
                            }
                            h5.fillDataDimensions(dataSetId, false, dataSetInfo);
                            return dataSetInfo;
                        }
                    };
        return runner.call(informationDeterminationRunnable);
    }

    HDF5DataTypeVariant tryGetTypeVariant(final String objectPath)
    {
        assert objectPath != null;

        final ICallableWithCleanUp<HDF5DataTypeVariant> readRunnable =
                new ICallableWithCleanUp<HDF5DataTypeVariant>()
                    {
                        public HDF5DataTypeVariant call(ICleanUpRegistry registry)
                        {
                            final int objectId = h5.openObject(fileId, objectPath, registry);
                            return tryGetTypeVariant(objectId, registry);
                        }
                    };

        return runner.call(readRunnable);
    }

    HDF5EnumerationValueArray getEnumValueArray(final int attributeId, ICleanUpRegistry registry)
    {
        final int storageDataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
        final int nativeDataTypeId = h5.getNativeDataType(storageDataTypeId, registry);
        final int len;
        final int enumTypeId;
        if (h5.getClassType(storageDataTypeId) == H5T_ARRAY)
        {
            final int[] arrayDimensions = h5.getArrayDimensions(storageDataTypeId);
            if (arrayDimensions.length != 1)
            {
                throw new HDF5JavaException("Array needs to be of rank 1, but is of rank "
                        + arrayDimensions.length);
            }
            len = arrayDimensions[0];
            enumTypeId = h5.getBaseDataType(storageDataTypeId, registry);
            if (h5.getClassType(enumTypeId) != H5T_ENUM)
            {
                throw new HDF5JavaException("Attribute is not of type Enumeration array.");
            }
        } else
        {
            if (h5.getClassType(storageDataTypeId) != H5T_ENUM)
            {
                throw new HDF5JavaException("Attribute is not of type Enumeration array.");
            }
            enumTypeId = storageDataTypeId;
            final long[] arrayDimensions = h5.getDataDimensionsForAttribute(attributeId, registry);
            len = HDF5Utils.getOneDimensionalArraySize(arrayDimensions);
        }
        final HDF5EnumerationType enumType =
                getEnumTypeForStorageDataType(null, enumTypeId, fileRegistry);
        final byte[] data =
                h5.readAttributeAsByteArray(attributeId, nativeDataTypeId, len
                        * enumType.getStorageForm().getStorageSize());
        final HDF5EnumerationValueArray value =
                new HDF5EnumerationValueArray(enumType, HDF5EnumerationType.fromStorageForm(data,
                        enumType.getStorageForm()));
        return value;
    }

    int getEnumDataTypeId(final int storageDataTypeId, ICleanUpRegistry registry)
    {
        final int enumDataTypeId;
        if (h5.getClassType(storageDataTypeId) == H5T_ARRAY)
        {
            enumDataTypeId = h5.getBaseDataType(storageDataTypeId, registry);
        } else
        {
            enumDataTypeId = storageDataTypeId;
        }
        return enumDataTypeId;
    }

    HDF5DataTypeVariant[] tryGetTypeVariantForCompoundMembers(String dataTypePathOrNull,
            ICleanUpRegistry registry)
    {
        if (dataTypePathOrNull == null)
        {
            return null;
        }
        checkOpen();
        final int objectId = h5.openObject(fileId, dataTypePathOrNull, registry);
        if (h5.existsAttribute(objectId, TYPE_VARIANT_MEMBERS_ATTRIBUTE) == false)
        {
            return null;
        }
        final int attributeId =
                h5.openAttribute(objectId, TYPE_VARIANT_MEMBERS_ATTRIBUTE, registry);
        final HDF5EnumerationValueArray valueArray = getEnumValueArray(attributeId, registry);
        final HDF5DataTypeVariant[] variants = new HDF5DataTypeVariant[valueArray.getLength()];
        boolean hasVariants = false;
        for (int i = 0; i < variants.length; ++i)
        {
            variants[i] = HDF5DataTypeVariant.values()[valueArray.getOrdinal(i)];
            hasVariants |= variants[i].isTypeVariant();
        }
        if (hasVariants)
        {
            return variants;
        } else
        {
            return null;
        }
    }

    HDF5DataTypeVariant tryGetTypeVariant(final int objectId, ICleanUpRegistry registry)
    {
        final int typeVariantOrdinal = getAttributeTypeVariant(objectId, registry);
        return typeVariantOrdinal < 0 ? null : HDF5DataTypeVariant.values()[typeVariantOrdinal];
    }

    /**
     * Returns the ordinal for the type variant of <var>objectPath</var>, or <code>-1</code>, if no
     * type variant is defined for this <var>objectPath</var>.
     * 
     * @param objectId The id of the data set object in the file.
     * @return The ordinal of the type variant or <code>null</code>.
     */
    int getAttributeTypeVariant(final int objectId, ICleanUpRegistry registry)
    {
        checkOpen();
        if (h5.existsAttribute(objectId, TYPE_VARIANT_ATTRIBUTE) == false)
        {
            return -1;
        }
        final int attributeId = h5.openAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, registry);
        return getEnumOrdinal(attributeId, -1, typeVariantDataType);
    }

    int getEnumOrdinal(final int attributeId, int nativeDataTypeId,
            final HDF5EnumerationType enumType)
    {
        final byte[] data =
                h5.readAttributeAsByteArray(attributeId,
                        (nativeDataTypeId < 0) ? enumType.getNativeTypeId() : nativeDataTypeId,
                        enumType.getStorageForm().getStorageSize());
        return HDF5EnumerationType.fromStorageForm(data);
    }

    HDF5DataTypeInformation getDataTypeInformation(final int dataTypeId,
            final ICleanUpRegistry registry)
    {
        final int classTypeId = h5.getClassType(dataTypeId);
        final HDF5DataClass dataClass;
        final int totalSize = h5.getDataTypeSize(dataTypeId);
        if (classTypeId == H5T_ARRAY)
        {
            dataClass = getElementClassForArrayDataType(dataTypeId);
            final int[] arrayDimensions = h5.getArrayDimensions(dataTypeId);
            final int numberOfElements = MDArray.getLength(arrayDimensions);
            final int size = totalSize / numberOfElements;
            final int baseTypeId = h5.getBaseDataType(dataTypeId, registry);
            final String dataTypePathOrNull = tryGetDataTypePath(baseTypeId);
            return new HDF5DataTypeInformation(dataTypePathOrNull, dataClass, size,
                    arrayDimensions, true);
        } else
        {
            dataClass = getDataClassForClassType(classTypeId, dataTypeId);
            final String opaqueTagOrNull;
            if (dataClass == HDF5DataClass.OPAQUE)
            {
                opaqueTagOrNull = h5.tryGetOpaqueTag(dataTypeId);
            } else
            {
                opaqueTagOrNull = null;
            }
            final String dataTypePathOrNull = tryGetDataTypePath(dataTypeId);
            return new HDF5DataTypeInformation(dataTypePathOrNull, dataClass, totalSize, 1,
                    opaqueTagOrNull);
        }
    }

    private HDF5DataClass getDataClassForClassType(final int classTypeId, final int dataTypeId)
    {
        HDF5DataClass dataClass = HDF5DataClass.classIdToDataClass(classTypeId);
        // Is it a boolean?
        if (dataClass == HDF5DataClass.ENUM && h5.dataTypesAreEqual(dataTypeId, booleanDataTypeId))
        {
            dataClass = HDF5DataClass.BOOLEAN;
        }
        return dataClass;
    }

    private HDF5DataClass getElementClassForArrayDataType(final int arrayDataTypeId)
    {
        for (HDF5DataClass eClass : HDF5DataClass.values())
        {
            if (h5.hasClassType(arrayDataTypeId, eClass.getId()))
            {
                return eClass;
            }
        }
        return HDF5DataClass.OTHER;
    }

    //
    // Compound
    //

    String getCompoundDataTypeName(final String nameOrNull, final int dataTypeId)
    {
        return getDataTypeName(nameOrNull, HDF5DataClass.COMPOUND, dataTypeId);
    }

    <T> HDF5ValueObjectByteifyer<T> createCompoundByteifyers(final Class<T> compoundClazz,
            final HDF5CompoundMemberMapping[] compoundMembers)
    {
        final HDF5ValueObjectByteifyer<T> objectByteifyer =
                new HDF5ValueObjectByteifyer<T>(compoundClazz,
                        new HDF5ValueObjectByteifyer.FileInfoProvider()
                            {
                                public int getBooleanDataTypeId()
                                {
                                    return booleanDataTypeId;
                                }

                                public int getStringDataTypeId(int maxLength)
                                {
                                    final int typeId =
                                            h5.createDataTypeString(maxLength, fileRegistry);
                                    return typeId;
                                }

                                public int getArrayTypeId(int baseTypeId, int length)
                                {
                                    final int typeId =
                                            h5.createArrayType(baseTypeId, length, fileRegistry);
                                    return typeId;
                                }

                                public int getArrayTypeId(int baseTypeId, int[] dimensions)
                                {
                                    final int typeId =
                                            h5.createArrayType(baseTypeId, dimensions, fileRegistry);
                                    return typeId;
                                }

                                public CharacterEncoding getCharacterEncoding()
                                {
                                    return encoding;
                                }
                            }, compoundMembers);
        return objectByteifyer;
    }

    int createStorageCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int storageDataTypeId =
                h5.createDataTypeCompound(objectArrayifyer.getRecordSize(), fileRegistry);
        objectArrayifyer.insertMemberTypes(storageDataTypeId);
        return storageDataTypeId;
    }

    int createNativeCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int nativeDataTypeId =
                h5.createDataTypeCompound(objectArrayifyer.getRecordSize(), fileRegistry);
        objectArrayifyer.insertNativeMemberTypes(nativeDataTypeId, h5, fileRegistry);
        return nativeDataTypeId;
    }

    //
    // Enum
    //

    HDF5EnumerationType getEnumTypeForStorageDataType(final String nameOrNull,
            final int storageDataTypeId, final ICleanUpRegistry registry)
    {
        return getEnumTypeForStorageDataType(nameOrNull, storageDataTypeId, true, registry);
    }

    HDF5EnumerationType getEnumTypeForStorageDataType(final String nameOrNull,
            final int storageDataTypeId, final boolean resolveName, final ICleanUpRegistry registry)
    {
        final int nativeDataTypeId = h5.getNativeDataType(storageDataTypeId, registry);
        final String[] values = h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(fileId, storageDataTypeId, nativeDataTypeId,
                resolveName ? getEnumDataTypeName(nameOrNull, storageDataTypeId) : nameOrNull,
                values);
    }

    void checkEnumValues(int dataTypeId, final String[] values, final String nameOrNull)
    {
        final String[] valuesStored = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
        if (valuesStored.length != values.length)
        {
            throw new IllegalStateException("Enum " + getEnumDataTypeName(nameOrNull, dataTypeId)
                    + " has " + valuesStored.length + " members, but should have " + values.length);
        }
        for (int i = 0; i < values.length; ++i)
        {
            if (values[i].equals(valuesStored[i]) == false)
            {
                throw new HDF5JavaException("Enum member index " + i + " of enum "
                        + getEnumDataTypeName(nameOrNull, dataTypeId) + " is '" + valuesStored[i]
                        + "', but should be '" + values[i] + "'");
            }
        }
    }

    String getEnumDataTypeName(final String nameOrNull, final int dataTypeId)
    {
        return getDataTypeName(nameOrNull, HDF5DataClass.ENUM, dataTypeId);
    }

    private String getDataTypeName(final String nameOrNull, final HDF5DataClass dataClass,
            final int dataTypeId)
    {
        if (nameOrNull != null)
        {
            return nameOrNull;
        } else
        {
            final String nameFromPathOrNull =
                    HDF5Utils.tryGetDataTypeNameFromPath(tryGetDataTypePath(dataTypeId), dataClass);
            return (nameFromPathOrNull == null) ? "UNKNOWN" : nameFromPathOrNull;
        }
    }

    boolean isScaledEnum(final int objectId, final ICleanUpRegistry registry)
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectId, registry);
        return (HDF5DataTypeVariant.ENUM == typeVariantOrNull);
    }

    //
    // String
    //

    String getStringAttribute(final int objectId, final String objectPath,
            final String attributeName, final ICleanUpRegistry registry)
    {
        final int attributeId = h5.openAttribute(objectId, attributeName, registry);
        final int stringDataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
        final boolean isString = (h5.getClassType(stringDataTypeId) == H5T_STRING);
        if (isString == false)
        {
            throw new IllegalArgumentException("Attribute " + attributeName + " of object "
                    + objectPath + " needs to be a String.");
        }
        final int size = h5.getDataTypeSize(stringDataTypeId);
        if (h5.isVariableLengthString(stringDataTypeId))
        {
            String[] data = new String[1];
            h5.readAttributeVL(attributeId, stringDataTypeId, data);
            return data[0];
        } else
        {
            byte[] data = h5.readAttributeAsByteArray(attributeId, stringDataTypeId, size);
            return StringUtils.fromBytes0Term(data, encoding);
        }
    }

    String[] getStringArrayAttribute(final int objectId, final String objectPath,
            final String attributeName, final ICleanUpRegistry registry)
    {
        final int attributeId = h5.openAttribute(objectId, attributeName, registry);
        final int stringArrayDataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
        final boolean isArray = (h5.getClassType(stringArrayDataTypeId) == H5T_ARRAY);
        if (isArray == false)
        {
            throw new HDF5JavaException("Attribute " + attributeName + " of object " + objectPath
                    + " needs to be a String array of rank 1.");
        }
        final int stringDataTypeId = h5.getBaseDataType(stringArrayDataTypeId, registry);
        final boolean isStringArray = (h5.getClassType(stringDataTypeId) == H5T_STRING);
        if (isStringArray == false)
        {
            throw new HDF5JavaException("Attribute " + attributeName + " of object " + objectPath
                    + " needs to be a String array of rank 1.");
        }
        final int size = h5.getDataTypeSize(stringArrayDataTypeId);
        if (h5.isVariableLengthString(stringDataTypeId))
        {
            String[] data = new String[1];
            h5.readAttributeVL(attributeId, stringDataTypeId, data);
            return data;
        } else
        {
            byte[] data = h5.readAttributeAsByteArray(attributeId, stringArrayDataTypeId, size);
            final int[] arrayDimensions = h5.getArrayDimensions(stringArrayDataTypeId);
            if (arrayDimensions.length != 1)
            {
                throw new HDF5JavaException("Attribute " + attributeName + " of object "
                        + objectPath + " needs to be a String array of rank 1.");
            }
            final int lengthPerElement = h5.getDataTypeSize(stringDataTypeId);
            final int numberOfElements = arrayDimensions[0];
            final String[] result = new String[numberOfElements];
            for (int i = 0; i < numberOfElements; ++i)
            {
                final int startIdx = i * lengthPerElement;
                final int maxEndIdx = startIdx + lengthPerElement;
                result[i] = StringUtils.fromBytes0Term(data, startIdx, maxEndIdx, encoding);
            }
            return result;
        }
    }

    MDArray<String> getStringMDArrayAttribute(final int objectId, final String objectPath,
            final String attributeName, final ICleanUpRegistry registry)
    {
        final int attributeId = h5.openAttribute(objectId, attributeName, registry);
        final int stringArrayDataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
        final boolean isArray = (h5.getClassType(stringArrayDataTypeId) == H5T_ARRAY);
        if (isArray == false)
        {
            throw new HDF5JavaException("Attribute " + attributeName + " of object " + objectPath
                    + " needs to be a String array.");
        }
        final int stringDataTypeId = h5.getBaseDataType(stringArrayDataTypeId, registry);
        final boolean isStringArray = (h5.getClassType(stringDataTypeId) == H5T_STRING);
        if (isStringArray == false)
        {
            throw new HDF5JavaException("Attribute " + attributeName + " of object " + objectPath
                    + " needs to be a String array.");
        }
        final int size = h5.getDataTypeSize(stringArrayDataTypeId);
        if (h5.isVariableLengthString(stringDataTypeId))
        {
            String[] data = new String[1];
            h5.readAttributeVL(attributeId, stringDataTypeId, data);
            return new MDArray<String>(data, new int[]
                { 1 });
        } else
        {
            byte[] data = h5.readAttributeAsByteArray(attributeId, stringArrayDataTypeId, size);
            final int[] arrayDimensions = h5.getArrayDimensions(stringArrayDataTypeId);
            final int lengthPerElement = h5.getDataTypeSize(stringDataTypeId);
            final int numberOfElements = MDArray.getLength(arrayDimensions);
            final String[] result = new String[numberOfElements];
            for (int i = 0; i < numberOfElements; ++i)
            {
                final int startIdx = i * lengthPerElement;
                final int maxEndIdx = startIdx + lengthPerElement;
                result[i] = StringUtils.fromBytes0Term(data, startIdx, maxEndIdx, encoding);
            }
            return new MDArray<String>(result, arrayDimensions);
        }
    }

    // Date & Time

    void checkIsTimeStamp(final String objectPath, final int dataSetId, ICleanUpRegistry registry)
            throws HDF5JavaException
    {
        final int typeVariantOrdinal = getAttributeTypeVariant(dataSetId, registry);
        if (typeVariantOrdinal != HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH
                .ordinal())
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time stamp.");
        }
    }

    HDF5TimeUnit checkIsTimeDuration(final String objectPath, final int dataSetId,
            ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int typeVariantOrdinal = getAttributeTypeVariant(dataSetId, registry);
        if (HDF5DataTypeVariant.isTimeDuration(typeVariantOrdinal) == false)
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time duration.");
        }
        return HDF5DataTypeVariant.getTimeUnit(typeVariantOrdinal);
    }

}
