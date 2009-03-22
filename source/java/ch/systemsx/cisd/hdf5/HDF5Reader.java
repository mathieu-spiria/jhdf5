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

import static ch.systemsx.cisd.hdf5.HDF5Utils.ENUM_PREFIX;
import static ch.systemsx.cisd.hdf5.HDF5Utils.createDataTypePath;
import static ch.systemsx.cisd.hdf5.HDF5Utils.getOneDimensionalArraySize;
import static ch.systemsx.cisd.hdf5.HDF5Utils.removeInternalNames;
import static ncsa.hdf.hdf5lib.H5.H5Tdetect_class;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_BITFIELD;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_COMPOUND;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ENUM;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_INTEGER;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_OPAQUE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_DOUBLE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STRING;
import java.io.File;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import ncsa.hdf.hdf5lib.HDFNativeData;
import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.common.array.MDArray;
import ch.systemsx.cisd.common.array.MDByteArray;
import ch.systemsx.cisd.common.array.MDDoubleArray;
import ch.systemsx.cisd.common.array.MDFloatArray;
import ch.systemsx.cisd.common.array.MDIntArray;
import ch.systemsx.cisd.common.array.MDLongArray;
import ch.systemsx.cisd.common.array.MDShortArray;
import ch.systemsx.cisd.common.process.ICallableWithCleanUp;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;

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
public class HDF5Reader implements HDF5SimpleReader, IHDF5PrimitiveReader
{

    private static final int MIN_ENUM_SIZE_FOR_UPFRONT_LOADING = 10;

    private final HDF5BaseReader baseReader;

    private final IHDF5ByteReader byteReader;

    private final IHDF5ShortReader shortReader;

    private final IHDF5IntReader intReader;

    private final IHDF5LongReader longReader;

    private final IHDF5FloatReader floatReader;

    private final IHDF5DoubleReader doubleReader;

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
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    /**
     * Returns <code>true</code>, if the latest file format will be used and <code>false</code>, if
     * a file format with maximum compatibility will be used.
     */
    public boolean isPerformNumericConversions()
    {
        return baseReader.performNumericConversions;
    }

    /**
     * Returns the HDF5 file that this class is reading.
     */
    public File getFile()
    {
        return baseReader.hdf5File;
    }

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    public void close()
    {
        baseReader.close();
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    /**
     * Returns the link information for the given <var>objectPath</var>. You need to ensure that the
     * link given by <var>objectPath</var> exists, e.g. by calling
     * {@link HDF5LinkInformation#checkExists()} first.
     * 
     * @throws ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException If the link specified by
     *             <var>objectPath</var> doesn't exist.
     */
    public HDF5LinkInformation getLinkInformation(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getLinkInfo(baseReader.fileId, objectPath, false);
    }

    /**
     * Returns the type of the given <var>objectPath</var>.
     */
    public HDF5ObjectType getObjectType(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getTypeInfo(baseReader.fileId, objectPath, false);
    }

    /**
     * Returns <code>true</code>, if <var>objectPath</var> exists and <code>false</code> otherwise.
     */
    public boolean exists(final String objectPath)
    {
        baseReader.checkOpen();
        if ("/".equals(objectPath))
        {
            return true;
        }
        return baseReader.h5.exists(baseReader.fileId, objectPath);
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a group and
     * <code>false</code> otherwise.
     */
    public boolean isGroup(final String objectPath)
    {
        return HDF5ObjectType.isGroup(getObjectType(objectPath));
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data set and
     * <code>false</code> otherwise.
     */
    public boolean isDataSet(final String objectPath)
    {
        return HDF5ObjectType.isDataSet(getObjectType(objectPath));
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data type and
     * <code>false</code> otherwise.
     */
    public boolean isDataType(final String objectPath)
    {
        return HDF5ObjectType.isDataType(getObjectType(objectPath));
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a soft link and
     * <code>false</code> otherwise.
     */
    public boolean isSoftLink(final String objectPath)
    {
        return HDF5ObjectType.isSoftLink(getObjectType(objectPath));
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a soft link and
     * <code>false</code> otherwise.
     */
    public boolean isExternalLink(final String objectPath)
    {
        return HDF5ObjectType.isExternalLink(getObjectType(objectPath));
    }

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a soft link and
     * <code>false</code> otherwise.
     */
    public boolean isSymbolicLink(final String objectPath)
    {
        return HDF5ObjectType.isSymbolicLink(getObjectType(objectPath));
    }

    /**
     * Returns the path of the data type of the data set <var>objectPath</var>, or <code>null</code>
     * , if this data set is not of a named data type.
     */
    public String tryGetDataTypePath(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> dataTypeNameCallable =
                new ICallableWithCleanUp<String>()
                    {
                        public String call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int dataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            return baseReader.h5.tryGetDataTypePath(dataTypeId);
                        }
                    };
        return baseReader.runner.call(dataTypeNameCallable);
    }

    /**
     * Returns the path of the data <var>type</var>, or <code>null</code>, if <var>type</var> is not
     * a named data type.
     */
    public String tryGetDataTypePath(HDF5DataType type)
    {
        assert type != null;

        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return baseReader.h5.tryGetDataTypePath(type.getStorageTypeId());
    }

    /**
     * Returns the names of the attributes of the given <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the object (data set or group) to
     *            return the attributes for.
     */
    public List<String> getAttributeNames(final String objectPath)
    {
        assert objectPath != null;
        baseReader.checkOpen();
        return removeInternalNames(getAllAttributeNames(objectPath));
    }

    /**
     * Returns the names of all attributes of the given <var>objectPath</var>.
     * <p>
     * This may include attributes that are used internally by the library and are not supposed to
     * be changed by application programmers.
     * 
     * @param objectPath The name (including path information) of the object (data set or group) to
     *            return the attributes for.
     */
    public List<String> getAllAttributeNames(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<List<String>> attributeNameReaderRunnable =
                new ICallableWithCleanUp<List<String>>()
                    {
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

    /**
     * Returns the information about a data set as a {@link HDF5DataTypeInformation} object.
     * 
     * @param dataSetPath The name (including path information) of the data set to return
     *            information about.
     * @param attributeName The name of the attribute to get information about.
     */
    public HDF5DataTypeInformation getAttributeInformation(final String dataSetPath,
            final String attributeName)
    {
        assert dataSetPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5DataTypeInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataTypeInformation>()
                    {
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
                                return baseReader.getDataTypeInformation(dataTypeId);
                            } catch (RuntimeException ex)
                            {
                                throw ex;
                            }
                        }
                    };
        return baseReader.runner.call(informationDeterminationRunnable);
    }

    /**
     * Returns the information about a data set as a {@link HDF5DataTypeInformation} object. It is a
     * failure condition if the <var>dataSetPath</var> does not exist or does not identify a data
     * set.
     * 
     * @param dataSetPath The name (including path information) of the data set to return
     *            information about.
     */
    public HDF5DataSetInformation getDataSetInformation(final String dataSetPath)
    {
        assert dataSetPath != null;

        baseReader.checkOpen();
        return baseReader.getDataSetInformation(dataSetPath);
    }

    // /////////////////////
    // Group
    // /////////////////////

    /**
     * Returns the members of <var>groupPath</var>. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMembers(groupPath);
    }

    /**
     * Returns all members of <var>groupPath</var>, including internal groups that may be used by
     * the library to do house-keeping. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getAllGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getAllGroupMembers(groupPath);
    }

    /**
     * Returns the paths of the members of <var>groupPath</var> (including the parent). The order is
     * <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the member paths for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getGroupMemberPaths(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMemberPaths(groupPath);
    }

    /**
     * Returns the link information about the members of <var>groupPath</var>. The order is
     * <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @param readLinkTargets If <code>true</code>, for symbolic links the link targets will be
     *            available via {@link HDF5LinkInformation#tryGetSymbolicLinkTarget()}.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<HDF5LinkInformation> getGroupMemberInformation(final String groupPath,
            boolean readLinkTargets)
    {
        baseReader.checkOpen();
        if (readLinkTargets)
        {
            return baseReader.h5.getGroupMemberLinkInfo(baseReader.fileId, groupPath, false);
        } else
        {
            return baseReader.h5.getGroupMemberTypeInfo(baseReader.fileId, groupPath, false);
        }
    }

    /**
     * Returns the link information about all members of <var>groupPath</var>. The order is
     * <i>not</i> well defined.
     * <p>
     * This may include attributes that are used internally by the library and are not supposed to
     * be changed by application programmers.
     * 
     * @param groupPath The path of the group to get the members for.
     * @param readLinkTargets If <code>true</code>, the link targets will be read for symbolic
     *            links.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<HDF5LinkInformation> getAllGroupMemberInformation(final String groupPath,
            boolean readLinkTargets)
    {
        baseReader.checkOpen();
        if (readLinkTargets)
        {
            return baseReader.h5.getGroupMemberLinkInfo(baseReader.fileId, groupPath, true);
        } else
        {
            return baseReader.h5.getGroupMemberTypeInfo(baseReader.fileId, groupPath, true);
        }
    }

    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Returns the tag of the opaque data type associated with <var>objectPath</var>, or
     * <code>null</code>, if <var>objectPath</var> is not of an opaque data type (i.e. if
     * 
     * <code>reader.getDataSetInformation(objectPath).getTypeInformation().getDataClass() != HDF5DataClass.OPAQUE</code>
     * ).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The tag of the opaque data type, or <code>null</code>.
     */
    public String tryGetOpaqueTag(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readTagCallable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int dataTypeId = baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    return baseReader.h5.getOpaqueTag(dataTypeId);
                }
            };
        return baseReader.runner.call(readTagCallable);
    }

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Use this method only when
     * you know that the type exists.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     */
    public HDF5EnumerationType getEnumType(final String name)
    {
        baseReader.checkOpen();
        final String dataTypePath = createDataTypePath(ENUM_PREFIX, name);
        final int storageDataTypeId = baseReader.getDataTypeId(dataTypePath);
        final int nativeDataTypeId =
                baseReader.h5.getNativeDataType(storageDataTypeId, baseReader.fileRegistry);
        final String[] values = baseReader.h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                name, values);
    }

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Will check the type in the
     * file with the <var>values</var>.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return getEnumType(name, values, true);
    }

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @param check If <code>true</code> and if the data type already exists, check whether it is
     *            compatible with the <var>values</var> provided.
     * @throws HDF5JavaException If <code>check = true</code>, the data type exists and is not
     *             compatible with the <var>values</var> provided.
     */
    public HDF5EnumerationType getEnumType(final String name, final String[] values,
            final boolean check) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final HDF5EnumerationType dataType = getEnumType(name);
        if (check)
        {
            checkEnumValues(dataType.getStorageTypeId(), values, name);
        }
        return dataType;
    }

    protected void checkEnumValues(int dataTypeId, final String[] values, final String nameOrNull)
    {
        final String[] valuesStored = baseReader.h5.getNamesForEnumOrCompoundMembers(dataTypeId);
        if (valuesStored.length != values.length)
        {
            throw new IllegalStateException("Enum "
                    + getCompoundDataTypeName(nameOrNull, dataTypeId) + " has "
                    + valuesStored.length + " members, but should have " + values.length);
        }
        for (int i = 0; i < values.length; ++i)
        {
            if (values[i].equals(valuesStored[i]) == false)
            {
                throw new HDF5JavaException("Enum member index " + i + " of enum "
                        + getCompoundDataTypeName(nameOrNull, dataTypeId) + " is '"
                        + valuesStored[i] + "', but should be " + values[i]);
            }
        }
    }

    private String getCompoundDataTypeName(final String nameOrNull, final int dataTypeId)
    {
        if (nameOrNull != null)
        {
            return nameOrNull;
        } else
        {
            final String path = baseReader.h5.tryGetDataTypePath(dataTypeId);
            if (path == null)
            {
                return "UNKNOWN";
            } else
            {
                return path.substring(HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX)
                        .length());
            }
        }
    }

    /**
     * Returns the enumeration type for the data set <var>dataSetPath</var>.
     * 
     * @param dataSetPath The name of data set to get the enumeration type for.
     */
    public HDF5EnumerationType getEnumTypeForObject(final String dataSetPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationType> readEnumTypeCallable =
                new ICallableWithCleanUp<HDF5EnumerationType>()
                    {
                        public HDF5EnumerationType call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, dataSetPath,
                                            registry);
                            return getEnumTypeForDataSetId(dataSetId, dataSetPath, isScaledEnum(
                                    dataSetId, registry), registry);
                        }
                    };
        return baseReader.runner.call(readEnumTypeCallable);
    }

    private HDF5EnumerationType getEnumTypeForDataSetId(final int objectId,
            final String objectName, final boolean scaledEnum, final ICleanUpRegistry registry)
    {
        if (scaledEnum)
        {
            final String enumTypeName =
                    getStringAttribute(objectId, objectName, HDF5Utils.ENUM_TYPE_NAME_ATTRIBUTE,
                            registry);
            return getEnumType(enumTypeName);
        } else
        {
            final int storageDataTypeId =
                    baseReader.h5.getDataTypeForDataSet(objectId, baseReader.fileRegistry);
            final int nativeDataTypeId =
                    baseReader.h5.getNativeDataType(storageDataTypeId, baseReader.fileRegistry);
            final String[] values =
                    baseReader.h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
            return new HDF5EnumerationType(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                    null, values);
        }
    }

    private boolean isScaledEnum(final int objectId, final ICleanUpRegistry registry)
    {
        final HDF5DataTypeVariant typeVariantOrNull =
                baseReader.tryGetTypeVariant(objectId, registry);
        return (HDF5DataTypeVariant.ENUM == typeVariantOrNull);

    }

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Returns <code>true</code>, if the <var>objectPath</var> has an attribute with name
     * <var>attributeName</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return <code>true</code>, if the attribute exists for the object.
     */
    public boolean hasAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return baseReader.h5.existsAttribute(objectId, attributeName);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a <code>String</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public String getStringAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    return getStringAttribute(objectId, objectPath, attributeName, registry);
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    private String getStringAttribute(final int objectId, final String objectPath,
            final String attributeName, final ICleanUpRegistry registry)
    {
        final int attributeId = baseReader.h5.openAttribute(objectId, attributeName, registry);
        final int dataTypeId = baseReader.h5.getDataTypeForAttribute(attributeId, registry);
        final boolean isString = (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
        if (isString == false)
        {
            throw new IllegalArgumentException("Attribute " + attributeName + " of object "
                    + objectPath + " needs to be a String.");
        }
        final int size = baseReader.h5.getDataTypeSize(dataTypeId);
        final int stringDataTypeId = baseReader.h5.createDataTypeString(size, registry);
        byte[] data = baseReader.h5.readAttributeAsByteArray(attributeId, stringDataTypeId, size);
        int termIdx;
        for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
        {
        }
        return new String(data, 0, termIdx);
    }

    /**
     * Reads a <code>boolean</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @throws HDF5JavaException If the attribute is not a boolean type.
     */
    public boolean getBooleanAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForAttribute(attributeId, registry);
                    byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, nativeDataTypeId, 1);
                    final Boolean value =
                            baseReader.h5.tryGetBooleanValue(nativeDataTypeId, data[0]);
                    if (value == null)
                    {
                        throw new HDF5JavaException("Attribute " + attributeName + " of path "
                                + objectPath + " needs to be a Boolean.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads an <code>enum</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set as a String.
     * @throws HDF5JavaException If the attribute is not an enum type.
     */
    public String getEnumAttributeAsString(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForAttribute(attributeId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, nativeDataTypeId, 4);
                    final String value =
                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(storageDataTypeId,
                                    HDFNativeData.byteToInt(data, 0));
                    if (value == null)
                    {
                        throw new HDF5JavaException("Attribute " + attributeName + " of path "
                                + objectPath + " needs to be an Enumeration.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(readRunnable);
    }

    /**
     * Returns the data type variant of <var>objectPath</var>, or <code>null</code>, if no type
     * variant is defined for this <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data type variant or <code>null</code>.
     */
    public HDF5DataTypeVariant tryGetTypeVariant(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5DataTypeVariant> readRunnable =
                new ICallableWithCleanUp<HDF5DataTypeVariant>()
                    {
                        public HDF5DataTypeVariant call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            return baseReader.tryGetTypeVariant(objectId, registry);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    /**
     * Reads an <code>enum</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @throws HDF5JavaException If the attribute is not an enum type.
     */
    public HDF5EnumerationValue getEnumAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseReader.h5.openObject(baseReader.fileId, objectPath,
                                            registry);
                            final int attributeId =
                                    baseReader.h5.openAttribute(objectId, attributeName, registry);
                            final HDF5EnumerationType enumType =
                                    getEnumTypeForAttributeId(attributeId);
                            final int enumOrdinal =
                                    baseReader.getEnumOrdinal(attributeId, enumType);
                            return new HDF5EnumerationValue(enumType, enumOrdinal);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    private HDF5EnumerationType getEnumTypeForAttributeId(final int objectId)
    {
        final int storageDataTypeId =
                baseReader.h5.getDataTypeForAttribute(objectId, baseReader.fileRegistry);
        final int nativeDataTypeId =
                baseReader.h5.getNativeDataType(storageDataTypeId, baseReader.fileRegistry);
        final String[] values = baseReader.h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                null, values);
    }

    /**
     * Reads an <code>int</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public int getIntegerAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> writeRunnable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, H5T_NATIVE_INT32, 4);
                    return HDFNativeData.byteToInt(data, 0);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a <code>long</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public long getLongAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> writeRunnable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, H5T_NATIVE_INT64, 8);
                    return HDFNativeData.byteToLong(data, 0);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a <code>float</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public float getFloatAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Float> writeRunnable = new ICallableWithCleanUp<Float>()
            {
                public Float call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5
                                    .readAttributeAsByteArray(attributeId, H5T_NATIVE_FLOAT, 4);
                    return HDFNativeData.byteToFloat(data, 0);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a <code>double</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public double getDoubleAttribute(final String objectPath, final String attributeName)
    {
        assert objectPath != null;
        assert attributeName != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Double> writeRunnable = new ICallableWithCleanUp<Double>()
            {
                public Double call(ICleanUpRegistry registry)
                {
                    final int objectId =
                            baseReader.h5.openObject(baseReader.fileId, objectPath, registry);
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final byte[] data =
                            baseReader.h5.readAttributeAsByteArray(attributeId, H5T_NATIVE_DOUBLE,
                                    8);
                    return HDFNativeData.byteToDouble(data, 0);
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    //
    // Generic
    //

    /**
     * Reads the data set <var>objectPath</var> as byte array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public byte[] readAsByteArray(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSetCheckBitFields(dataSetId,
                                    registry);
                    final byte[] data =
                            new byte[(spaceParams.blockSize == 0 ? 1 : spaceParams.blockSize)
                                    * baseReader.h5.getDataTypeSize(nativeDataTypeId)];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * <em>Must not be called for data sets of rank higher than 1!</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code>
     *            returned).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public byte[] readAsByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * <em>Must not be called for data sets of rank higher than 1!</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code>
     *            returned).
     * @param offset The offset of the block to read (starting with 0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public byte[] readAsByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * <em>Must not be called for data sets of rank higher than 1!</em>
     * <p>
     * <b>Note:</b> If the data set has a rank higher than 1, then the natural block size will be
     * chosen to be the complete data set, even when the data set is chunked with a smaller chunk
     * size!
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<byte[]>> getAsByteArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<byte[]>>()
            {
                public Iterator<HDF5DataBlock<byte[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<byte[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<byte[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final byte[] block =
                                        readAsByteArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<byte[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    //
    // Boolean
    //

    /**
     * Reads a <code>Boolean</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a boolean type.
     */
    public boolean readBoolean(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final byte[] data = new byte[1];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                    final Boolean value =
                            baseReader.h5.tryGetBooleanValue(nativeDataTypeId, data[0]);
                    if (value == null)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a Boolean.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a bit field (which can be considered the equivalent to a boolean array of rank 1) from
     * the data set <var>objectPath</var> and returns it as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by
     * {@link HDF5Writer#writeLongArray(String, long[])} cannot be read back by this method but will
     * throw a {@link HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The {@link BitSet} read from the data set.
     * @throws HDF5DatatypeInterfaceException If the <var>objectPath</var> is not of bit field type.
     */
    public BitSet readBitField(final String objectPath) throws HDF5DatatypeInterfaceException
    {
        baseReader.checkOpen();
        return BitSetConversionUtils.fromStorageForm(readBitFieldStorageForm(objectPath));
    }

    /**
     * Reads a <code>long</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    private long[] readBitFieldStorageForm(final String objectPath)
    {
        assert objectPath != null;

        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_B64, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - START
    // ------------------------------------------------------------------------------

    public Iterable<HDF5DataBlock<byte[]>> getByteArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return byteReader.getByteArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDByteArray>> getByteMDArrayNaturalBlocks(String dataSetPath)
    {
        return byteReader.getByteMDArrayNaturalBlocks(dataSetPath);
    }

    public byte readByte(String objectPath)
    {
        return byteReader.readByte(objectPath);
    }

    public byte[] readByteArray(String objectPath)
    {
        return byteReader.readByteArray(objectPath);
    }

    public byte[] readByteArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return byteReader.readByteArrayBlock(objectPath, blockSize, blockNumber);
    }

    public byte[] readByteArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return byteReader.readByteArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDByteArray readByteMDArray(String objectPath)
    {
        return byteReader.readByteMDArray(objectPath);
    }

    public MDByteArray readByteMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return byteReader.readByteMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDByteArray readByteMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return byteReader.readByteMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public byte[][] readByteMatrix(String objectPath) throws HDF5JavaException
    {
        return byteReader.readByteMatrix(objectPath);
    }

    public byte[][] readByteMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return byteReader.readByteMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public byte[][] readByteMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return byteReader.readByteMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    public void readToByteMDArrayBlockWithOffset(String objectPath, MDByteArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        byteReader.readToByteMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToByteMDArrayWithOffset(String objectPath, MDByteArray array, int[] memoryOffset)
    {
        byteReader.readToByteMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    public Iterable<HDF5DataBlock<double[]>> getDoubleArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return doubleReader.getDoubleArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDDoubleArray>> getDoubleMDArrayNaturalBlocks(String dataSetPath)
    {
        return doubleReader.getDoubleMDArrayNaturalBlocks(dataSetPath);
    }

    public double readDouble(String objectPath)
    {
        return doubleReader.readDouble(objectPath);
    }

    public double[] readDoubleArray(String objectPath)
    {
        return doubleReader.readDoubleArray(objectPath);
    }

    public double[] readDoubleArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return doubleReader.readDoubleArrayBlock(objectPath, blockSize, blockNumber);
    }

    public double[] readDoubleArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return doubleReader.readDoubleArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDDoubleArray readDoubleMDArray(String objectPath)
    {
        return doubleReader.readDoubleMDArray(objectPath);
    }

    public MDDoubleArray readDoubleMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return doubleReader.readDoubleMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDDoubleArray readDoubleMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return doubleReader.readDoubleMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public double[][] readDoubleMatrix(String objectPath) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrix(objectPath);
    }

    public double[][] readDoubleMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public double[][] readDoubleMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return doubleReader.readDoubleMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    public void readToDoubleMDArrayBlockWithOffset(String objectPath, MDDoubleArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        doubleReader.readToDoubleMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToDoubleMDArrayWithOffset(String objectPath, MDDoubleArray array,
            int[] memoryOffset)
    {
        doubleReader.readToDoubleMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    public Iterable<HDF5DataBlock<float[]>> getFloatArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return floatReader.getFloatArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDFloatArray>> getFloatMDArrayNaturalBlocks(String dataSetPath)
    {
        return floatReader.getFloatMDArrayNaturalBlocks(dataSetPath);
    }

    public float readFloat(String objectPath)
    {
        return floatReader.readFloat(objectPath);
    }

    public float[] readFloatArray(String objectPath)
    {
        return floatReader.readFloatArray(objectPath);
    }

    public float[] readFloatArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return floatReader.readFloatArrayBlock(objectPath, blockSize, blockNumber);
    }

    public float[] readFloatArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return floatReader.readFloatArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDFloatArray readFloatMDArray(String objectPath)
    {
        return floatReader.readFloatMDArray(objectPath);
    }

    public MDFloatArray readFloatMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return floatReader.readFloatMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDFloatArray readFloatMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return floatReader.readFloatMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public float[][] readFloatMatrix(String objectPath) throws HDF5JavaException
    {
        return floatReader.readFloatMatrix(objectPath);
    }

    public float[][] readFloatMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return floatReader.readFloatMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public float[][] readFloatMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return floatReader.readFloatMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    public void readToFloatMDArrayBlockWithOffset(String objectPath, MDFloatArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        floatReader.readToFloatMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToFloatMDArrayWithOffset(String objectPath, MDFloatArray array,
            int[] memoryOffset)
    {
        floatReader.readToFloatMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return intReader.getIntArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(String dataSetPath)
    {
        return intReader.getIntMDArrayNaturalBlocks(dataSetPath);
    }

    public int readInt(String objectPath)
    {
        return intReader.readInt(objectPath);
    }

    public int[] readIntArray(String objectPath)
    {
        return intReader.readIntArray(objectPath);
    }

    public int[] readIntArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return intReader.readIntArrayBlock(objectPath, blockSize, blockNumber);
    }

    public int[] readIntArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return intReader.readIntArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDIntArray readIntMDArray(String objectPath)
    {
        return intReader.readIntMDArray(objectPath);
    }

    public MDIntArray readIntMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return intReader.readIntMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDIntArray readIntMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return intReader.readIntMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public int[][] readIntMatrix(String objectPath) throws HDF5JavaException
    {
        return intReader.readIntMatrix(objectPath);
    }

    public int[][] readIntMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return intReader.readIntMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public int[][] readIntMatrixBlockWithOffset(String objectPath, int blockSizeX, int blockSizeY,
            long offsetX, long offsetY) throws HDF5JavaException
    {
        return intReader.readIntMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY, offsetX,
                offsetY);
    }

    public void readToIntMDArrayBlockWithOffset(String objectPath, MDIntArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        intReader.readToIntMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToIntMDArrayWithOffset(String objectPath, MDIntArray array, int[] memoryOffset)
    {
        intReader.readToIntMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    public Iterable<HDF5DataBlock<long[]>> getLongArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return longReader.getLongArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDLongArray>> getLongMDArrayNaturalBlocks(String dataSetPath)
    {
        return longReader.getLongMDArrayNaturalBlocks(dataSetPath);
    }

    public long readLong(String objectPath)
    {
        return longReader.readLong(objectPath);
    }

    public long[] readLongArray(String objectPath)
    {
        return longReader.readLongArray(objectPath);
    }

    public long[] readLongArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return longReader.readLongArrayBlock(objectPath, blockSize, blockNumber);
    }

    public long[] readLongArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return longReader.readLongArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDLongArray readLongMDArray(String objectPath)
    {
        return longReader.readLongMDArray(objectPath);
    }

    public MDLongArray readLongMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return longReader.readLongMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDLongArray readLongMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return longReader.readLongMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public long[][] readLongMatrix(String objectPath) throws HDF5JavaException
    {
        return longReader.readLongMatrix(objectPath);
    }

    public long[][] readLongMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return longReader.readLongMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public long[][] readLongMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return longReader.readLongMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    public void readToLongMDArrayBlockWithOffset(String objectPath, MDLongArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        longReader.readToLongMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToLongMDArrayWithOffset(String objectPath, MDLongArray array, int[] memoryOffset)
    {
        longReader.readToLongMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    public Iterable<HDF5DataBlock<short[]>> getShortArrayNaturalBlocks(String dataSetPath)
            throws HDF5JavaException
    {
        return shortReader.getShortArrayNaturalBlocks(dataSetPath);
    }

    public Iterable<HDF5MDDataBlock<MDShortArray>> getShortMDArrayNaturalBlocks(String dataSetPath)
    {
        return shortReader.getShortMDArrayNaturalBlocks(dataSetPath);
    }

    public short readShort(String objectPath)
    {
        return shortReader.readShort(objectPath);
    }

    public short[] readShortArray(String objectPath)
    {
        return shortReader.readShortArray(objectPath);
    }

    public short[] readShortArrayBlock(String objectPath, int blockSize, long blockNumber)
    {
        return shortReader.readShortArrayBlock(objectPath, blockSize, blockNumber);
    }

    public short[] readShortArrayBlockWithOffset(String objectPath, int blockSize, long offset)
    {
        return shortReader.readShortArrayBlockWithOffset(objectPath, blockSize, offset);
    }

    public MDShortArray readShortMDArray(String objectPath)
    {
        return shortReader.readShortMDArray(objectPath);
    }

    public MDShortArray readShortMDArrayBlock(String objectPath, int[] blockDimensions,
            long[] blockNumber)
    {
        return shortReader.readShortMDArrayBlock(objectPath, blockDimensions, blockNumber);
    }

    public MDShortArray readShortMDArrayBlockWithOffset(String objectPath, int[] blockDimensions,
            long[] offset)
    {
        return shortReader.readShortMDArrayBlockWithOffset(objectPath, blockDimensions, offset);
    }

    public short[][] readShortMatrix(String objectPath) throws HDF5JavaException
    {
        return shortReader.readShortMatrix(objectPath);
    }

    public short[][] readShortMatrixBlock(String objectPath, int blockSizeX, int blockSizeY,
            long blockNumberX, long blockNumberY) throws HDF5JavaException
    {
        return shortReader.readShortMatrixBlock(objectPath, blockSizeX, blockSizeY, blockNumberX,
                blockNumberY);
    }

    public short[][] readShortMatrixBlockWithOffset(String objectPath, int blockSizeX,
            int blockSizeY, long offsetX, long offsetY) throws HDF5JavaException
    {
        return shortReader.readShortMatrixBlockWithOffset(objectPath, blockSizeX, blockSizeY,
                offsetX, offsetY);
    }

    public void readToShortMDArrayBlockWithOffset(String objectPath, MDShortArray array,
            int[] blockDimensions, long[] offset, int[] memoryOffset)
    {
        shortReader.readToShortMDArrayBlockWithOffset(objectPath, array, blockDimensions, offset,
                memoryOffset);
    }

    public void readToShortMDArrayWithOffset(String objectPath, MDShortArray array,
            int[] memoryOffset)
    {
        shortReader.readToShortMDArrayWithOffset(objectPath, array, memoryOffset);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - END
    // ------------------------------------------------------------------------------

    //
    // Time stamp
    //

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time stamp and
     * <code>false</code> otherwise.
     */
    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeStamp();
    }

    /**
     * Reads a time stamp value from the data set <var>objectPath</var>. The time stamp is stored as
     * a <code>long</code> value in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     */
    public long readTimeStamp(final String objectPath) throws HDF5JavaException
    {
        baseReader.checkOpen();
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    checkIsTimeStamp(objectPath, dataSetId, registry);
                    final long[] data = new long[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a time stamp array from the data set <var>objectPath</var>. The time stamp is stored as
     * a <code>long</code> value in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     */
    public long[] readTimeStampArray(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    checkIsTimeStamp(objectPath, dataSetId, registry);
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

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public long[] readTimeStampArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    checkIsTimeStamp(objectPath, dataSetId, registry);
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

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public long[] readTimeStampArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    checkIsTimeStamp(objectPath, dataSetId, registry);
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

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<long[]>> getTimeStampArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<long[]>>()
            {
                public Iterator<HDF5DataBlock<long[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<long[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<long[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final long[] block =
                                        readTimeStampArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<long[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    /**
     * Reads a time stamp value from the data set <var>objectPath</var> and returns it as a
     * {@link Date}. The time stamp is stored as a <code>long</code> value in the HDF5 file. It
     * needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as {@link Date}.
     * @throws HDF5JavaException If the <var>objectPath</var> does not denote a time stamp.
     */
    public Date readDate(final String objectPath) throws HDF5JavaException
    {
        return new Date(readTimeStamp(objectPath));
    }

    /**
     * Reads a time stamp array (of rank 1) from the data set <var>objectPath</var> and returns it
     * as an array of {@link Date}s. The time stamp array is stored as a an array of
     * <code>long</code> values in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as {@link Date}.
     * @throws HDF5JavaException If the <var>objectPath</var> does not denote a time stamp.
     */
    public Date[] readDateArray(final String objectPath) throws HDF5JavaException
    {
        final long[] timeStampArray = readTimeStampArray(objectPath);
        return timeStampsToDates(timeStampArray);
    }

    /**
     * Converts an array of time stamps into an array of {@link Date}s.
     */
    public static Date[] timeStampsToDates(final long[] timeStampArray)
    {
        assert timeStampArray != null;

        final Date[] dateArray = new Date[timeStampArray.length];
        for (int i = 0; i < dateArray.length; ++i)
        {
            dateArray[i] = new Date(timeStampArray[i]);
        }
        return dateArray;
    }

    protected void checkIsTimeStamp(final String objectPath, final int dataSetId,
            ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int typeVariantOrdinal = baseReader.getAttributeTypeVariant(dataSetId, registry);
        if (typeVariantOrdinal != HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH
                .ordinal())
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time stamp.");
        }
    }

    //
    // Duration
    //

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     */
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeDuration();
    }

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     */
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return (typeVariantOrNull != null) ? typeVariantOrNull.tryGetTimeUnit() : null;
    }

    /**
     * Reads a time duration value from the data set <var>objectPath</var>, converts it to seconds
     * and returns it as <code>long</code>. It needs to be tagged as one of the type variants that
     * indicate a time duration, for example {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration in seconds.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @see #readTimeDuration(String, HDF5TimeUnit)
     */
    public long readTimeDuration(final String objectPath) throws HDF5JavaException
    {
        return readTimeDuration(objectPath, HDF5TimeUnit.SECONDS);
    }

    /**
     * Reads a time duration value from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged as one of the
     * type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link HDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link HDF5Writer#addTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The time duration in the given unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public long readTimeDuration(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final long[] data = new long[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return timeUnit.convert(data[0], storedUnit);
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a time duration array from the data set <var>objectPath</var>, converts it to seconds
     * and returns it as a <code>long[]</code> array. It needs to be tagged as one of the type
     * variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration in seconds.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @see #readTimeDurationArray(String, HDF5TimeUnit)
     */
    public long[] readTimeDurationArray(final String objectPath) throws HDF5JavaException
    {
        return readTimeDurationArray(objectPath, HDF5TimeUnit.SECONDS);
    }

    /**
     * Reads a time duration array from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as a <code>long[]</code> array. It needs to be tagged as
     * one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The time duration in the given unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     */
    public long[] readTimeDurationArray(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    convertTimeDurations(timeUnit, storedUnit, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public long[] readTimeDurationArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final HDF5TimeUnit timeUnit)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    convertTimeDurations(timeUnit, storedUnit, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of a time stamp array (of rank 1) from the data set <var>objectPath</var>. The
     * time stamp is stored as a <code>long</code> value in the HDF5 file. It needs to be tagged as
     * type variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data block read from the data set.
     */
    public long[] readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final HDF5TimeUnit storedUnit =
                            checkIsTimeDuration(objectPath, dataSetId, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final long[] data = new long[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    convertTimeDurations(timeUnit, storedUnit, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @param timeUnit The time unit that the duration should be converted to.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(
            final String dataSetPath, final HDF5TimeUnit timeUnit) throws HDF5JavaException
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<long[]>>()
            {
                public Iterator<HDF5DataBlock<long[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<long[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<long[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final long[] block =
                                        readTimeDurationArrayBlockWithOffset(dataSetPath,
                                                blockSize, offset, timeUnit);
                                return new HDF5DataBlock<long[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    protected HDF5TimeUnit checkIsTimeDuration(final String objectPath, final int dataSetId,
            ICleanUpRegistry registry) throws HDF5JavaException
    {
        final int typeVariantOrdinal = baseReader.getAttributeTypeVariant(dataSetId, registry);
        if (HDF5DataTypeVariant.isTimeDuration(typeVariantOrdinal) == false)
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time duration.");
        }
        return HDF5DataTypeVariant.getTimeUnit(typeVariantOrdinal);
    }

    static void convertTimeDurations(final HDF5TimeUnit timeUnit, final HDF5TimeUnit storedUnit,
            final long[] data)
    {
        if (timeUnit != storedUnit)
        {
            for (int i = 0; i < data.length; ++i)
            {
                data[i] = timeUnit.convert(data[i], storedUnit);
            }
        }
    }

    //
    // String
    //

    /**
     * Reads a <code>String</code> from the data set <var>objectPath</var>. This needs to be a
     * string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String readString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
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
                        int termIdx;
                        for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
                        {
                        }
                        return new String(data, 0, termIdx);
                    }
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads a <code>String</code> array (of rank 1) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArray(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                    final String[] data = new String[getOneDimensionalArraySize(dimensions)];
                    final int dataTypeId =
                            baseReader.h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final boolean isString = (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a String.");
                    }
                    baseReader.h5.readDataSetNonNumeric(dataSetId, dataTypeId, data);
                    return data;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    //
    // Enum
    //

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set as a String.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public String readEnumAsString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);
                    final int size = baseReader.h5.getDataTypeSize(nativeDataTypeId);
                    final String value;
                    switch (size)
                    {
                        case 1:
                        {
                            final byte[] data = new byte[1];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                            storageDataTypeId, data[0]);
                            break;
                        }
                        case 2:
                        {
                            final short[] data = new short[1];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                            storageDataTypeId, data[0]);
                            break;
                        }
                        case 4:
                        {
                            final int[] data = new int[1];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                            storageDataTypeId, data[0]);
                            break;
                        }
                        default:
                            throw new HDF5JavaException("Unexpected size for Enum data type ("
                                    + size + ")");
                    }
                    if (value == null)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be an Enumeration.");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValue readEnum(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final HDF5EnumerationType enumType =
                                    getEnumTypeForDataSetId(dataSetId, objectPath, false, registry);
                            return readEnumValue(dataSetId, enumType);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * <p>
     * This method is faster than {@link #readEnum(String)} if the {@link HDF5EnumerationType} is
     * already available.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enum type in the HDF5 file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValue readEnum(final String objectPath, final HDF5EnumerationType enumType)
            throws HDF5JavaException
    {
        assert objectPath != null;
        assert enumType != null;

        baseReader.checkOpen();
        enumType.check(baseReader.fileId);
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            return readEnumValue(dataSetId, enumType);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    private HDF5EnumerationValue readEnumValue(final int dataSetId,
            final HDF5EnumerationType enumType)
    {
        switch (enumType.getStorageForm())
        {
            case BYTE:
            {
                final byte[] data = new byte[1];
                baseReader.h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
                return new HDF5EnumerationValue(enumType, data[0]);
            }
            case SHORT:
            {
                final short[] data = new short[1];
                baseReader.h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
                return new HDF5EnumerationValue(enumType, data[0]);
            }
            case INT:
            {
                final int[] data = new int[1];
                baseReader.h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
                return new HDF5EnumerationValue(enumType, data[0]);
            }
            default:
                throw new HDF5JavaException("Illegal storage form for enum ("
                        + enumType.getStorageForm() + ")");
        }
    }

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath,
            final HDF5EnumerationType enumType) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValueArray> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValueArray>()
                    {
                        public HDF5EnumerationValueArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                            final boolean scaledEnum = isScaledEnum(dataSetId, registry);
                            final HDF5EnumerationType actualEnumType =
                                    (enumType == null) ? getEnumTypeForDataSetId(dataSetId,
                                            objectPath, scaledEnum, registry) : enumType;
                            final int arraySize = HDF5Utils.getOneDimensionalArraySize(dimensions);
                            switch (actualEnumType.getStorageForm())
                            {
                                case BYTE:
                                {
                                    final byte[] data = new byte[arraySize];
                                    if (scaledEnum)
                                    {
                                        baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, data);
                                    } else
                                    {
                                        baseReader.h5.readDataSet(dataSetId, actualEnumType
                                                .getNativeTypeId(), data);
                                    }
                                    return new HDF5EnumerationValueArray(actualEnumType, data);
                                }
                                case SHORT:
                                {
                                    final short[] data = new short[arraySize];
                                    if (scaledEnum)
                                    {
                                        baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16, data);
                                    } else
                                    {
                                        baseReader.h5.readDataSet(dataSetId, actualEnumType
                                                .getNativeTypeId(), data);
                                    }
                                    return new HDF5EnumerationValueArray(actualEnumType, data);
                                }
                                case INT:
                                {
                                    final int[] data = new int[arraySize];
                                    if (scaledEnum)
                                    {
                                        baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, data);
                                    } else
                                    {
                                        baseReader.h5.readDataSet(dataSetId, actualEnumType
                                                .getNativeTypeId(), data);
                                    }
                                    return new HDF5EnumerationValueArray(actualEnumType, data);
                                }
                            }
                            throw new Error("Illegal storage form ("
                                    + actualEnumType.getStorageForm() + ".)");
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath)
            throws HDF5JavaException
    {
        return readEnumArray(objectPath, null);
    }

    /**
     * Reads an <code>Enum</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set as an array of Strings.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public String[] readEnumArrayAsString(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] dimensions = baseReader.h5.getDataDimensions(dataSetId);
                    final int vectorLength = getOneDimensionalArraySize(dimensions);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    final int nativeDataTypeId =
                            baseReader.h5.getNativeDataType(storageDataTypeId, registry);
                    final HDF5EnumerationType enumTypeOrNull =
                            tryGetEnumTypeForResolution(dataSetId, objectPath, nativeDataTypeId,
                                    vectorLength, registry);
                    final int size = baseReader.h5.getDataTypeSize(nativeDataTypeId);

                    final String[] value = new String[vectorLength];
                    switch (size)
                    {
                        case 1:
                        {
                            final byte[] data = new byte[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            if (enumTypeOrNull != null)
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] = enumTypeOrNull.getValueArray()[data[i]];
                                }
                            } else
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] =
                                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                    storageDataTypeId, data[i]);
                                }
                            }
                            break;
                        }
                        case 2:
                        {
                            final short[] data = new short[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            if (enumTypeOrNull != null)
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] = enumTypeOrNull.getValueArray()[data[i]];
                                }
                            } else
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] =
                                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                    storageDataTypeId, data[i]);
                                }
                            }
                            break;
                        }
                        case 4:
                        {
                            final int[] data = new int[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            if (enumTypeOrNull != null)
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] = enumTypeOrNull.getValueArray()[data[i]];
                                }
                            } else
                            {
                                for (int i = 0; i < data.length; ++i)
                                {
                                    value[i] =
                                            baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                    storageDataTypeId, data[i]);
                                }
                            }
                            break;
                        }
                        default:
                            throw new HDF5JavaException("Unexpected size for Enum data type ("
                                    + size + ")");
                    }
                    return value;
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private HDF5EnumerationType tryGetEnumTypeForResolution(final int dataSetId,
            final String objectPath, final int nativeDataTypeId,
            final int numberOfEntriesToResolve, ICleanUpRegistry registry)
    {
        final boolean nativeEnum = (baseReader.h5.getClassType(nativeDataTypeId) == H5T_ENUM);
        final boolean scaledEnum = nativeEnum ? false : isScaledEnum(dataSetId, registry);
        if (nativeEnum == false && scaledEnum == false)
        {
            throw new HDF5JavaException(objectPath + " is not an enum.");
        }
        if (scaledEnum || numberOfEntriesToResolve >= MIN_ENUM_SIZE_FOR_UPFRONT_LOADING)
        {
            return getEnumTypeForDataSetId(dataSetId, objectPath, scaledEnum, registry);
        }
        return null;
    }

    //
    // Compound
    //

    /**
     * Returns the member information for the committed compound data type <var>compoundClass</var>
     * (using its "simple name"). The returned array will contain the members in alphabetical order.
     * It is a failure condition if this compound data type does not exist.
     */
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass)
    {
        return getCompoundMemberInformation(compoundClass.getSimpleName());
    }

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>.
     * The returned array will contain the members in alphabetical order. It is a failure condition
     * if this compound data type does not exist.
     */
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(final String dataTypeName)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> writeRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            final String dataTypePath =
                                    HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX,
                                            dataTypeName);
                            final int compoundDataTypeId =
                                    baseReader.h5.openDataType(baseReader.fileId, dataTypePath,
                                            registry);
                            return getCompoundMemberInformation(compoundDataTypeId, registry);
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var>. The returned
     * array will contain the members in alphabetical order. It is a failure condition if this data
     * set does not exist or is not of compound type.
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     */
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> writeRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, dataSetPath,
                                            registry);
                            final int compoundDataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            if (baseReader.h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
                            {
                                throw new HDF5JavaException("Data set '" + dataSetPath
                                        + "' is not of compound type.");
                            }
                            return getCompoundMemberInformation(compoundDataTypeId, registry);
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

    private HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final int compoundDataTypeId, final ICleanUpRegistry registry)
    {
        final String[] memberNames =
                baseReader.h5.getNamesForEnumOrCompoundMembers(compoundDataTypeId);
        final HDF5CompoundMemberInformation[] memberInfo =
                new HDF5CompoundMemberInformation[memberNames.length];
        for (int i = 0; i < memberInfo.length; ++i)
        {
            final int dataTypeId =
                    baseReader.h5.getDataTypeForIndex(compoundDataTypeId, i, registry);
            final int sizeInBytes = baseReader.h5.getDataTypeSize(dataTypeId);
            final int dataClassTypeId = baseReader.h5.getClassType(dataTypeId);
            if (dataClassTypeId == H5T_ARRAY)
            {
                final int numberOfElements = getNumberOfArrayElements(dataTypeId);
                final int elementSize = sizeInBytes / numberOfElements;
                memberInfo[i] =
                        new HDF5CompoundMemberInformation(memberNames[i],
                                new HDF5DataTypeInformation(findDataClassForArray(dataTypeId),
                                        elementSize, numberOfElements));
            } else
            {
                memberInfo[i] =
                        new HDF5CompoundMemberInformation(memberNames[i],
                                new HDF5DataTypeInformation(baseReader.getDataClassForClassType(
                                        dataClassTypeId, dataTypeId), sizeInBytes));
            }
        }
        Arrays.sort(memberInfo);
        return memberInfo;
    }

    private int getNumberOfArrayElements(int arrayDataTypeId)
    {
        final int[] dims = baseReader.h5.getArrayDimensions(arrayDataTypeId);
        int numberOfElements = 1;
        for (int d : dims)
        {
            numberOfElements *= d;
        }
        return numberOfElements;
    }

    private HDF5DataClass findDataClassForArray(int arrayDataTypeId)
    {
        if (H5Tdetect_class(arrayDataTypeId, H5T_FLOAT))
        {
            return HDF5DataClass.FLOAT;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_INTEGER))
        {
            return HDF5DataClass.INTEGER;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_STRING))
        {
            return HDF5DataClass.STRING;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_BITFIELD))
        {
            return HDF5DataClass.BITFIELD;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_COMPOUND))
        {
            return HDF5DataClass.COMPOUND;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_ENUM))
        {
            return HDF5DataClass.ENUM;
        } else if (H5Tdetect_class(arrayDataTypeId, H5T_OPAQUE))
        {
            return HDF5DataClass.OPAQUE;
        } else
        {
            return HDF5DataClass.OTHER;
        }

    }

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        final HDF5ValueObjectByteifyer<T> objectArrayifyer =
                createByteifyers(compoundType, members);
        final int storageDataTypeId = createStorageCompoundDataType(objectArrayifyer);
        final int nativeDataTypeId = createNativeCompoundDataType(objectArrayifyer);
        return new HDF5CompoundType<T>(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                name, compoundType, objectArrayifyer);
    }

    protected int createStorageCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int storageDataTypeId =
                baseReader.h5.createDataTypeCompound(objectArrayifyer.getRecordSize(),
                        baseReader.fileRegistry);
        objectArrayifyer.insertMemberTypes(storageDataTypeId);
        return storageDataTypeId;
    }

    protected int createNativeCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int nativeDataTypeId =
                baseReader.h5.createDataTypeCompound(objectArrayifyer.getRecordSize(),
                        baseReader.fileRegistry);
        objectArrayifyer.insertNativeMemberTypes(nativeDataTypeId, baseReader.h5,
                baseReader.fileRegistry);
        return nativeDataTypeId;
    }

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        return getCompoundType(null, compoundType, members);
    }

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound type.
     */
    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompound(objectPath, -1, -1, type);
    }

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, -1, -1, type);
    }

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber, final HDF5CompoundMemberMapping... members)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, blockSize * blockNumber, type);
    }

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param offset The offset of the block to read (starting with 0).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, offset, type);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param type The type definition of this compound type.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String dataSetPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        if (info.getRank() > 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + info.getRank() + ")");
        }
        final long longSize = info.getDimensions()[0];
        final int size = (int) longSize;
        if (size != longSize)
        {
            throw new HDF5JavaException("Data Set is too large (" + longSize + ")");
        }
        final int naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes()[0]
                        : size;
        final int sizeModNaturalBlockSize = size % naturalBlockSize;
        final long numberOfBlocks =
                (size / naturalBlockSize) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
        final int lastBlockSize =
                (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize;

        return new Iterable<HDF5DataBlock<T[]>>()
            {
                public Iterator<HDF5DataBlock<T[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<T[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<T[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final T[] block =
                                        readCompoundArrayBlockWithOffset(dataSetPath, type,
                                                blockSize, offset);
                                return new HDF5DataBlock<T[]>(block, index++, offset);
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                }
            };
    }

    private <T> T primReadCompound(final String objectPath, final int blockSize, final long offset,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        final ICallableWithCleanUp<T> writeRunnable = new ICallableWithCleanUp<T>()
            {
                public T call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                    return type.getObjectByteifyer().arrayifyScalar(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private <T> T[] primReadCompoundArray(final String objectPath, final int blockSize,
            final long offset, final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        final ICallableWithCleanUp<T[]> writeRunnable = new ICallableWithCleanUp<T[]>()
            {
                public T[] call(final ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int storageDataTypeId =
                            baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                    return type.getObjectByteifyer().arrayify(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private void checkCompoundType(final int dataTypeId, final String path,
            final ICleanUpRegistry registry)
    {
        final boolean isCompound = (baseReader.h5.getClassType(dataTypeId) == H5T_COMPOUND);
        if (isCompound == false)
        {
            throw new HDF5JavaException(path + " needs to be a Compound.");
        }
    }

    protected <T> HDF5ValueObjectByteifyer<T> createByteifyers(final Class<T> compoundClazz,
            final HDF5CompoundMemberMapping[] compoundMembers)
    {
        final HDF5ValueObjectByteifyer<T> objectByteifyer =
                new HDF5ValueObjectByteifyer<T>(compoundClazz,
                        new HDF5ValueObjectByteifyer.FileInfoProvider()
                            {
                                public int getBooleanDataTypeId()
                                {
                                    return baseReader.booleanDataTypeId;
                                }

                                public int getStringDataTypeId(int maxLength)
                                {
                                    final int typeId =
                                            baseReader.h5.createDataTypeString(maxLength,
                                                    baseReader.fileRegistry);
                                    return typeId;
                                }

                                public int getArrayTypeId(int baseTypeId, int length)
                                {
                                    final int typeId =
                                            baseReader.h5.createArrayType(baseTypeId, length,
                                                    baseReader.fileRegistry);
                                    return typeId;
                                }
                            }, compoundMembers);
        return objectByteifyer;
    }

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, null, null);
    }

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param blockNumber The number of the block to write along each axis.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockDimensions[i] * blockNumber[i];
        }
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset);
    }

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param offset The offset of the block to write in the data set along each axis.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset);
    }

    private <T> MDArray<T> primReadCompoundArrayRankN(final String objectPath,
            final HDF5CompoundType<T> type, final int[] dimensionsOrNull, final long[] offsetOrNull)
            throws HDF5JavaException
    {
        final ICallableWithCleanUp<MDArray<T>> writeRunnable =
                new ICallableWithCleanUp<MDArray<T>>()
                    {
                        public MDArray<T> call(final ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int storageDataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            checkCompoundType(storageDataTypeId, objectPath, registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offsetOrNull,
                                            dimensionsOrNull, registry);
                            final int nativeDataTypeId = type.getNativeTypeId();
                            final byte[] byteArr =
                                    new byte[spaceParams.blockSize
                                            * type.getObjectByteifyer().getRecordSize()];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, byteArr);
                            return new MDArray<T>(type.getObjectByteifyer().arrayify(
                                    storageDataTypeId, byteArr, type.getCompoundType()),
                                    spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

}
