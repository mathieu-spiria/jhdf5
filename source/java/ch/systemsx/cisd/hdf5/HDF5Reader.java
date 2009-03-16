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
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_ATTRIBUTE;
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
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STRING;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_VARIABLE;

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
public class HDF5Reader implements HDF5SimpleReader
{

    private final HDF5BaseReader baseReader;

    HDF5Reader(HDF5BaseReader baseReader)
    {
        assert baseReader != null;

        this.baseReader = baseReader;
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
                                return getDataTypeInformation(dataTypeId);
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
        final ICallableWithCleanUp<HDF5DataSetInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataSetInformation>()
                    {
                        public HDF5DataSetInformation call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, dataSetPath,
                                            registry);
                            final int dataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId, registry);
                            final HDF5DataTypeInformation dataTypeInfo =
                                    getDataTypeInformation(dataTypeId);
                            final HDF5DataSetInformation dataSetInfo =
                                    new HDF5DataSetInformation(dataTypeInfo, tryGetTypeVariant(
                                            dataSetId, registry));
                            // Is it a variable-length string?
                            final boolean vlString =
                                    (dataTypeInfo.getDataClass() == HDF5DataClass.STRING && baseReader.h5
                                            .isVariableLengthString(dataTypeId));
                            if (vlString)
                            {
                                dataTypeInfo.setElementSize(1);

                                dataSetInfo.setDimensions(new long[]
                                    { H5T_VARIABLE });
                                dataSetInfo.setMaxDimensions(new long[]
                                    { H5T_VARIABLE });
                                dataSetInfo.setStorageLayout(StorageLayout.VARIABLE_LENGTH);
                            } else
                            {
                                baseReader.h5.fillDataDimensions(dataSetId, false, dataSetInfo);
                            }
                            return dataSetInfo;
                        }
                    };
        return baseReader.runner.call(informationDeterminationRunnable);
    }

    private HDF5DataTypeInformation getDataTypeInformation(final int dataTypeId)
    {
        return new HDF5DataTypeInformation(getDataClassForDataType(dataTypeId), baseReader.h5
                .getDataTypeSize(dataTypeId));
    }

    private HDF5DataClass getDataClassForDataType(final int dataTypeId)
    {
        return getDataClassForClassType(baseReader.h5.getClassType(dataTypeId), dataTypeId);
    }

    private HDF5DataClass getDataClassForClassType(final int classTypeId, final int dataTypeId)
    {
        HDF5DataClass dataClass = HDF5DataClass.classIdToDataClass(classTypeId);
        // Is it a boolean?
        if (dataClass == HDF5DataClass.ENUM
                && baseReader.h5.dataTypesAreEqual(dataTypeId, baseReader.booleanDataTypeId))
        {
            dataClass = HDF5DataClass.BOOLEAN;
        }
        return dataClass;
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
                            return getEnumTypeForDataSetId(dataSetId);
                        }
                    };
        return baseReader.runner.call(readEnumTypeCallable);
    }

    private HDF5EnumerationType getEnumTypeForDataSetId(final int objectId)
    {
        final int storageDataTypeId =
                baseReader.h5.getDataTypeForDataSet(objectId, baseReader.fileRegistry);
        final int nativeDataTypeId =
                baseReader.h5.getNativeDataType(storageDataTypeId, baseReader.fileRegistry);
        final String[] values = baseReader.h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(baseReader.fileId, storageDataTypeId, nativeDataTypeId,
                null, values);
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
                    final int attributeId =
                            baseReader.h5.openAttribute(objectId, attributeName, registry);
                    final int dataTypeId =
                            baseReader.h5.getDataTypeForAttribute(attributeId, registry);
                    final boolean isString = (baseReader.h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new IllegalArgumentException("Attribute " + attributeName
                                + " of object " + objectPath + " needs to be a String.");
                    }
                    final int size = baseReader.h5.getDataTypeSize(dataTypeId);
                    final int stringDataTypeId = baseReader.h5.createDataTypeString(size, registry);
                    byte[] data =
                            baseReader.h5.readAttributeAsByteArray(attributeId, stringDataTypeId,
                                    size);
                    int termIdx;
                    for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
                    {
                    }
                    return new String(data, 0, termIdx);
                }
            };
        return baseReader.runner.call(readRunnable);
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
                            return tryGetTypeVariant(objectId, registry);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    private HDF5DataTypeVariant tryGetTypeVariant(final int dataSetId, ICleanUpRegistry registry)
    {
        final int typeVariantOrdinal = getAttributeTypeVariant(dataSetId, registry);
        return typeVariantOrdinal < 0 ? null : HDF5DataTypeVariant.values()[typeVariantOrdinal];
    }

    /**
     * Returns the ordinal for the type variant of <var>objectPath</var>, or <code>-1</code>, if no
     * type variant is defined for this <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The ordinal of the type variant or <code>null</code>.
     */
    private int getAttributeTypeVariant(final int objectId, ICleanUpRegistry registry)
    {
        baseReader.checkOpen();
        if (baseReader.h5.existsAttribute(objectId, TYPE_VARIANT_ATTRIBUTE) == false)
        {
            return -1;
        }
        final int attributeId =
                baseReader.h5.openAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, registry);
        return getEnumOrdinal(attributeId, baseReader.typeVariantDataType);
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
                            final int enumOrdinal = getEnumOrdinal(attributeId, enumType);
                            return new HDF5EnumerationValue(enumType, enumOrdinal);
                        }
                    };

        return baseReader.runner.call(readRunnable);
    }

    private int getEnumOrdinal(final int attributeId, final HDF5EnumerationType enumType)
    {
        final int enumOrdinal;
        switch (enumType.getStorageForm())
        {
            case BYTE:
            {
                final byte[] data =
                        baseReader.h5.readAttributeAsByteArray(attributeId, enumType
                                .getNativeTypeId(), 1);
                enumOrdinal = data[0];
                break;
            }
            case SHORT:
            {
                final byte[] data =
                        baseReader.h5.readAttributeAsByteArray(attributeId, enumType
                                .getNativeTypeId(), 2);
                enumOrdinal = HDFNativeData.byteToShort(data, 0);
                break;
            }
            case INT:
            {
                final byte[] data =
                        baseReader.h5.readAttributeAsByteArray(attributeId, enumType
                                .getNativeTypeId(), 4);
                enumOrdinal = HDFNativeData.byteToInt(data, 0);
                break;
            }
            default:
                throw new HDF5JavaException("Illegal storage form for enum ("
                        + enumType.getStorageForm() + ")");
        }
        return enumOrdinal;
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

    //
    // Byte
    //

    /**
     * Reads a <code>byte</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public byte readByte(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Byte> readCallable = new ICallableWithCleanUp<Byte>()
            {
                public Byte call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final byte[] data = new byte[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>byte</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public byte[] readByteArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>byte</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToByteMDArrayWithOffset(final String objectPath, final MDByteArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT8, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>byte</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT8, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>byte</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public byte[] readByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

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
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from <code>byte</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public byte[] readByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final byte[] data = new byte[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>byte</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public byte[][] readByteMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>byte</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public byte[][] readByteMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>byte</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public byte[][] readByteMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDByteArray array = readByteMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDByteArray readByteMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT8,
                                            registry);
                            final byte[] data = new byte[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDByteArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDByteArray readByteMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final byte[] dataBlock = new byte[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDByteArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDByteArray readByteMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final byte[] dataBlock = new byte[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT8,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDByteArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<byte[]>> getByteArrayNaturalBlocks(final String dataSetPath)
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
                                        readByteArrayBlockWithOffset(dataSetPath, blockSize, offset);
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

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDByteArray>> getByteMDArrayNaturalBlocks(
            final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDByteArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDByteArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDByteArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDByteArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDByteArray data =
                                        readByteMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDByteArray>(data, index.clone(), offset
                                        .clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }

    //
    // Short
    //

    /**
     * Reads a <code>short</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public short readShort(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Short> readCallable = new ICallableWithCleanUp<Short>()
            {
                public Short call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final short[] data = new short[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>short</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public short[] readShortArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final short[] data = new short[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>short</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToShortMDArrayWithOffset(final String objectPath, final MDShortArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT16, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>short</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToShortMDArrayBlockWithOffset(final String objectPath,
            final MDShortArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT16, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>short</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>short[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public short[] readShortArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final short[] data = new short[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from <code>short</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>short[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public short[] readShortArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final short[] data = new short[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>short</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public short[][] readShortMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDShortArray array = readShortMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>short</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public short[][] readShortMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDShortArray array = readShortMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>short</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public short[][] readShortMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDShortArray array = readShortMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDShortArray readShortMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDShortArray> readCallable =
                new ICallableWithCleanUp<MDShortArray>()
                    {
                        public MDShortArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT16,
                                            registry);
                            final short[] data = new short[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDShortArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDShortArray readShortMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDShortArray> readCallable =
                new ICallableWithCleanUp<MDShortArray>()
                    {
                        public MDShortArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final short[] dataBlock = new short[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDShortArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDShortArray readShortMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDShortArray> readCallable =
                new ICallableWithCleanUp<MDShortArray>()
                    {
                        public MDShortArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final short[] dataBlock = new short[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT16,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDShortArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<short[]>> getShortArrayNaturalBlocks(final String dataSetPath)
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

        return new Iterable<HDF5DataBlock<short[]>>()
            {
                public Iterator<HDF5DataBlock<short[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<short[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<short[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final short[] block =
                                        readShortArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<short[]>(block, index++, offset);
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
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDShortArray>> getShortMDArrayNaturalBlocks(
            final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDShortArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDShortArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDShortArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDShortArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDShortArray data =
                                        readShortMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDShortArray>(data, index.clone(),
                                        offset.clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }

    //
    // Int
    //

    /**
     * Reads a <code>int</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public int readInt(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Integer> readCallable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final int[] data = new int[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>int</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public int[] readIntArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>int</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToIntMDArrayWithOffset(final String objectPath, final MDIntArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>int</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>int</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>int[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public int[] readIntArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from <code>int</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>int[]</code> returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public int[] readIntArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int[] data = new int[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDIntArray array = readIntMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDIntArray readIntMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT32,
                                            registry);
                            final int[] data = new int[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDIntArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDIntArray readIntMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final int[] dataBlock = new int[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDIntArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDIntArray readIntMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final int[] dataBlock = new int[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT32,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDIntArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(final String dataSetPath)
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

        return new Iterable<HDF5DataBlock<int[]>>()
            {
                public Iterator<HDF5DataBlock<int[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<int[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<int[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final int[] block =
                                        readIntArrayBlockWithOffset(dataSetPath, blockSize, offset);
                                return new HDF5DataBlock<int[]>(block, index++, offset);
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
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDIntArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDIntArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDIntArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDIntArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDIntArray data =
                                        readIntMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDIntArray>(data, index.clone(), offset
                                        .clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }

    //
    // Long
    //

    /**
     * Reads a <code>long</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public long readLong(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final long[] data = new long[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>long</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public long[] readLongArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
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
     * Reads a multi-dimensional <code>long</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToLongMDArrayWithOffset(final String objectPath, final MDLongArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT64, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>long</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToLongMDArrayBlockWithOffset(final String objectPath, final MDLongArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT64, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>long</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public long[] readLongArrayBlock(final String objectPath, final int blockSize,
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
     * Reads a block from <code>long</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public long[] readLongArrayBlockWithOffset(final String objectPath, final int blockSize,
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
     * Reads a <code>long</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public long[][] readLongMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDLongArray array = readLongMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>long</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public long[][] readLongMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDLongArray array = readLongMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>long</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public long[][] readLongMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDLongArray array = readLongMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDLongArray readLongMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_INT64,
                                            registry);
                            final long[] data = new long[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDLongArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDLongArray readLongMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final long[] dataBlock = new long[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDLongArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDLongArray readLongMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final long[] dataBlock = new long[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_INT64,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDLongArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<long[]>> getLongArrayNaturalBlocks(final String dataSetPath)
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
                                        readLongArrayBlockWithOffset(dataSetPath, blockSize, offset);
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
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDLongArray>> getLongMDArrayNaturalBlocks(
            final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDLongArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDLongArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDLongArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDLongArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDLongArray data =
                                        readLongMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDLongArray>(data, index.clone(), offset
                                        .clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }

    //
    // Float
    //

    /**
     * Reads a <code>float</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public float readFloat(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Float> readCallable = new ICallableWithCleanUp<Float>()
            {
                public Float call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final float[] data = new float[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>float</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public float[] readFloatArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final float[] data = new float[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>float</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToFloatMDArrayWithOffset(final String objectPath, final MDFloatArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_FLOAT, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>float</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToFloatMDArrayBlockWithOffset(final String objectPath,
            final MDFloatArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_FLOAT, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>float</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public float[] readFloatArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final float[] data = new float[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from <code>float</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>float[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public float[] readFloatArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final float[] data = new float[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>float</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public float[][] readFloatMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDFloatArray array = readFloatMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>float</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public float[][] readFloatMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDFloatArray array = readFloatMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>float</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public float[][] readFloatMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException
    {
        final MDFloatArray array = readFloatMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDFloatArray readFloatMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDFloatArray> readCallable =
                new ICallableWithCleanUp<MDFloatArray>()
                    {
                        public MDFloatArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_FLOAT,
                                            registry);
                            final float[] data = new float[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDFloatArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDFloatArray readFloatMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDFloatArray> readCallable =
                new ICallableWithCleanUp<MDFloatArray>()
                    {
                        public MDFloatArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final float[] dataBlock = new float[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDFloatArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDFloatArray readFloatMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDFloatArray> readCallable =
                new ICallableWithCleanUp<MDFloatArray>()
                    {
                        public MDFloatArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final float[] dataBlock = new float[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDFloatArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<float[]>> getFloatArrayNaturalBlocks(final String dataSetPath)
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

        return new Iterable<HDF5DataBlock<float[]>>()
            {
                public Iterator<HDF5DataBlock<float[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<float[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<float[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final float[] block =
                                        readFloatArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<float[]>(block, index++, offset);
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
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDFloatArray>> getFloatMDArrayNaturalBlocks(
            final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDFloatArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDFloatArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDFloatArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDFloatArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDFloatArray data =
                                        readFloatMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDFloatArray>(data, index.clone(),
                                        offset.clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
    }

    //
    // Double
    //

    /**
     * Reads a <code>double</code> value from the data set <var>objectPath</var>. This method
     * doesn't check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public double readDouble(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Double> readCallable = new ICallableWithCleanUp<Double>()
            {
                public Double call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final double[] data = new double[1];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE, data);
                    return data[0];
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>double</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public double[] readDoubleArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, registry);
                    final double[] data = new double[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>double</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     */
    public void readToDoubleMDArrayWithOffset(final String objectPath, final MDDoubleArray array,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_DOUBLE, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block of the multi-dimensional <code>double</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     */
    public void readToDoubleMDArrayBlockWithOffset(final String objectPath,
            final MDDoubleArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<Void> readCallable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getBlockSpaceParameters(dataSetId, memoryOffset, array
                                    .dimensions(), offset, blockDimensions, registry);
                    final int nativeDataTypeId =
                            baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_DOUBLE, registry);
                    baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, array
                                    .getAsFlatArray());
                    return null; // Nothing to return.
                }
            };
        baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from a <code>double</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>double[]</code>
     *            returned if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be min(size - blockSize*blockNumber,
     *         blockSize).
     */
    public double[] readDoubleArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, blockNumber * blockSize,
                                    blockSize, registry);
                    final double[] data = new double[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a block from <code>double</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>double[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     */
    public double[] readDoubleArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId =
                            baseReader.h5.openDataSet(baseReader.fileId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            baseReader.getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final double[] data = new double[spaceParams.blockSize];
                    baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE,
                            spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                    return data;
                }
            };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a <code>double</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public double[][] readDoubleMatrix(final String objectPath) throws HDF5JavaException
    {
        final MDDoubleArray array = readDoubleMDArray(objectPath);
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>double</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>blockSizeX</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>blockSizeY</code>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public double[][] readDoubleMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException
    {
        final MDDoubleArray array = readDoubleMDArrayBlock(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { blockNumberX, blockNumberY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a <code>double</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of the block in the x dimension.
     * @param blockSizeY The size of the block in the y dimension.
     * @param offsetX The offset in x dimension in the data set to start reading from.
     * @param offsetY The offset in y dimension in the data set to start reading from.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public double[][] readDoubleMatrixBlockWithOffset(final String objectPath,
            final int blockSizeX, final int blockSizeY, final long offsetX, final long offsetY)
            throws HDF5JavaException
    {
        final MDDoubleArray array = readDoubleMDArrayBlockWithOffset(objectPath, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { offsetX, offsetY });
        if (array.rank() != 2)
        {
            throw new HDF5JavaException("Array is supposed to be of rank 2, but is of rank "
                    + array.rank());
        }
        return array.toMatrix();
    }

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public MDDoubleArray readDoubleMDArray(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDDoubleArray> readCallable =
                new ICallableWithCleanUp<MDDoubleArray>()
                    {
                        public MDDoubleArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, registry);
                            final int nativeDataTypeId =
                                    baseReader.getNativeDataTypeId(dataSetId, H5T_NATIVE_DOUBLE,
                                            registry);
                            final double[] data = new double[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, data);
                            return new MDDoubleArray(data, spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     */
    public MDDoubleArray readDoubleMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert blockNumber != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDDoubleArray> readCallable =
                new ICallableWithCleanUp<MDDoubleArray>()
                    {
                        public MDDoubleArray call(ICleanUpRegistry registry)
                        {
                            final long[] offset = new long[blockDimensions.length];
                            for (int i = 0; i < offset.length; ++i)
                            {
                                offset[i] = blockNumber[i] * blockDimensions[i];
                            }
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final double[] dataBlock = new double[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDDoubleArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     */
    public MDDoubleArray readDoubleMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset)
    {
        assert objectPath != null;
        assert blockDimensions != null;
        assert offset != null;

        baseReader.checkOpen();
        final ICallableWithCleanUp<MDDoubleArray> readCallable =
                new ICallableWithCleanUp<MDDoubleArray>()
                    {
                        public MDDoubleArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final DataSpaceParameters spaceParams =
                                    baseReader.getSpaceParameters(dataSetId, offset,
                                            blockDimensions, registry);
                            final double[] dataBlock = new double[spaceParams.blockSize];
                            baseReader.h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE,
                                    spaceParams.memorySpaceId, spaceParams.dataSpaceId, dataBlock);
                            return new MDDoubleArray(dataBlock, blockDimensions);
                        }
                    };
        return baseReader.runner.call(readCallable);
    }

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<double[]>> getDoubleArrayNaturalBlocks(final String dataSetPath)
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

        return new Iterable<HDF5DataBlock<double[]>>()
            {
                public Iterator<HDF5DataBlock<double[]>> iterator()
                {
                    return new Iterator<HDF5DataBlock<double[]>>()
                        {
                            long index = 0;

                            public boolean hasNext()
                            {
                                return index < numberOfBlocks;
                            }

                            public HDF5DataBlock<double[]> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final long offset = naturalBlockSize * index;
                                final int blockSize =
                                        (index == numberOfBlocks - 1) ? lastBlockSize
                                                : naturalBlockSize;
                                final double[] block =
                                        readDoubleArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                return new HDF5DataBlock<double[]>(block, index++, offset);
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
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     */
    public Iterable<HDF5MDDataBlock<MDDoubleArray>> getDoubleMDArrayNaturalBlocks(
            final String dataSetPath)
    {
        final HDF5DataSetInformation info = getDataSetInformation(dataSetPath);
        final int rank = info.getRank();
        final int[] size = MDArray.toInt(info.getDimensions());
        final int[] naturalBlockSize =
                (info.getStorageLayout() == StorageLayout.CHUNKED) ? info.tryGetChunkSizes() : size;
        final long[] numberOfBlocks = new long[rank];
        final int[] lastBlockSize = new int[rank];
        for (int i = 0; i < size.length; ++i)
        {
            final int sizeModNaturalBlockSize = size[i] % naturalBlockSize[i];
            numberOfBlocks[i] =
                    (size[i] / naturalBlockSize[i]) + (sizeModNaturalBlockSize != 0 ? 1 : 0);
            lastBlockSize[i] =
                    (sizeModNaturalBlockSize != 0) ? sizeModNaturalBlockSize : naturalBlockSize[i];
        }

        return new Iterable<HDF5MDDataBlock<MDDoubleArray>>()
            {
                public Iterator<HDF5MDDataBlock<MDDoubleArray>> iterator()
                {
                    return new Iterator<HDF5MDDataBlock<MDDoubleArray>>()
                        {
                            long[] index = new long[rank];

                            long[] offset = new long[rank];

                            int[] blockSize = naturalBlockSize.clone();

                            boolean indexCalculated = true;

                            public boolean hasNext()
                            {
                                if (indexCalculated)
                                {
                                    return true;
                                }
                                for (int i = index.length - 1; i >= 0; --i)
                                {
                                    ++index[i];
                                    if (index[i] < numberOfBlocks[i])
                                    {
                                        offset[i] += naturalBlockSize[i];
                                        if (index[i] == numberOfBlocks[i] - 1)
                                        {
                                            blockSize[i] = lastBlockSize[i];
                                        }
                                        indexCalculated = true;
                                        break;
                                    } else
                                    {
                                        index[i] = 0;
                                        offset[i] = 0;
                                        blockSize[i] = naturalBlockSize[i];
                                    }
                                }
                                return indexCalculated;
                            }

                            public HDF5MDDataBlock<MDDoubleArray> next()
                            {
                                if (hasNext() == false)
                                {
                                    throw new NoSuchElementException();
                                }
                                final MDDoubleArray data =
                                        readDoubleMDArrayBlockWithOffset(dataSetPath, blockSize,
                                                offset);
                                prepareNext();
                                return new HDF5MDDataBlock<MDDoubleArray>(data, index.clone(),
                                        offset.clone());
                            }

                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }

                            private void prepareNext()
                            {
                                indexCalculated = false;
                            }
                        };
                }
            };
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
        final int typeVariantOrdinal = getAttributeTypeVariant(dataSetId, registry);
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
        final int typeVariantOrdinal = getAttributeTypeVariant(dataSetId, registry);
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
                            final HDF5EnumerationType enumType = getEnumTypeForDataSetId(dataSetId);
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
                            final HDF5EnumerationType actualEnumType =
                                    (enumType == null) ? getEnumTypeForDataSetId(dataSetId)
                                            : enumType;
                            final int arraySize = HDF5Utils.getOneDimensionalArraySize(dimensions);
                            switch (actualEnumType.getStorageForm())
                            {
                                case BYTE:
                                {
                                    final byte[] data = new byte[arraySize];
                                    baseReader.h5.readDataSet(dataSetId, actualEnumType
                                            .getNativeTypeId(), data);
                                    return new HDF5EnumerationValueArray(actualEnumType, data);
                                }
                                case SHORT:
                                {
                                    final short[] data = new short[arraySize];
                                    baseReader.h5.readDataSet(dataSetId, actualEnumType
                                            .getNativeTypeId(), data);
                                    return new HDF5EnumerationValueArray(actualEnumType, data);
                                }
                                case INT:
                                {
                                    final int[] data = new int[arraySize];
                                    baseReader.h5.readDataSet(dataSetId, actualEnumType
                                            .getNativeTypeId(), data);
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
                    final boolean isEnum =
                            (baseReader.h5.getClassType(nativeDataTypeId) == H5T_ENUM);
                    if (isEnum == false)
                    {
                        throw new HDF5JavaException(objectPath + " is not an enum.");
                    }
                    final int size = baseReader.h5.getDataTypeSize(nativeDataTypeId);

                    final String[] value = new String[vectorLength];
                    switch (size)
                    {
                        case 1:
                        {
                            final byte[] data = new byte[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                storageDataTypeId, data[i]);
                            }
                            break;
                        }
                        case 2:
                        {
                            final short[] data = new short[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                storageDataTypeId, data[i]);
                            }
                            break;
                        }
                        case 4:
                        {
                            final int[] data = new int[vectorLength];
                            baseReader.h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        baseReader.h5.getNameForEnumOrCompoundMemberIndex(
                                                storageDataTypeId, data[i]);
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
                                new HDF5DataTypeInformation(getDataClassForClassType(
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
