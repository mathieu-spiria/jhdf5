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

import static ch.systemsx.cisd.hdf5.HDF5Utils.*;
import static ncsa.hdf.hdf5lib.HDF5Constants.*;

import java.io.File;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ch.systemsx.cisd.common.array.MDArray;
import ch.systemsx.cisd.common.array.MDByteArray;
import ch.systemsx.cisd.common.array.MDDoubleArray;
import ch.systemsx.cisd.common.array.MDFloatArray;
import ch.systemsx.cisd.common.array.MDIntArray;
import ch.systemsx.cisd.common.array.MDLongArray;
import ch.systemsx.cisd.common.array.MDShortArray;
import ch.systemsx.cisd.common.process.CleanUpCallable;
import ch.systemsx.cisd.common.process.CleanUpRegistry;
import ch.systemsx.cisd.common.process.ICallableWithCleanUp;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;

/**
 * A class for reading HDF5 files (HDF5 1.8.x and older).
 * <p>
 * The class focuses on ease of use instead of completeness. As a consequence not all features of a
 * valid HDF5 files can be read using this class, but only a subset. (All information written by
 * {@link HDF5Writer} can be read by this class.)
 * <p>
 * <em>Note: The reader needs to be opened (call to {@link #open()}) before being used and should be 
 * closed (call to {@link #close()}) to free its resources (e.g. cache).</em>
 * <p>
 * Usage:
 * 
 * <pre>
 * HDF5Reader reader = new HDF5Reader(&quot;test.h5&quot;).open();
 * float[] f = reader.readFloatArray(&quot;/some/path/dataset&quot;);
 * String s = reader.getAttributeString(&quot;/some/path/dataset&quot;, &quot;some key&quot;);
 * reader.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
public class HDF5Reader implements HDF5SimpleReader
{
    protected final File hdf5File;

    protected final HDF5 h5;

    protected final CleanUpCallable runner;

    protected final CleanUpRegistry fileRegistry;

    /** Map from named data types to ids. */
    private final Map<String, Integer> namedDataTypeMap;

    protected int fileId;

    protected int booleanDataTypeId;

    protected int typeVariantDataTypeId;

    private boolean closed;

    protected void checkOpen() throws HDF5JavaException
    {
        if (closed)
        {
            throw new HDF5JavaException("HDF5 file '" + hdf5File.getPath() + "' is closed.");
        }
    }

    /**
     * Opens an existing HDF5 file for reading.
     * 
     * @param hdf5File The HDF5 file to read from.
     */
    public HDF5Reader(File hdf5File)
    {
        assert hdf5File != null;

        this.runner = new CleanUpCallable();
        this.fileRegistry = new CleanUpRegistry();
        this.h5 = new HDF5(fileRegistry);
        this.namedDataTypeMap = new HashMap<String, Integer>();
        this.hdf5File = hdf5File.getAbsoluteFile();
    }

    /**
     * Returns the HDF5 file that this class is reading.
     */
    public File getFile()
    {
        return hdf5File;
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    /**
     * Opens the HDF5 file for reading. Do not try to call any read method before calling this
     * method.
     */
    public HDF5Reader open()
    {
        final String path = hdf5File.getAbsolutePath();
        if (hdf5File.exists() == false)
        {
            throw new IllegalArgumentException("The file " + path + " does not exit.");
        }
        this.fileId = h5.openFileReadOnly(path, fileRegistry);
        readNamedDataTypes();
        this.booleanDataTypeId = openOrCreateBooleanDataType();
        this.typeVariantDataTypeId = openOrCreateTypeVariantDataType();

        return this;
    }

    protected void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        // Overwrite method in writer.
    }

    protected int openOrCreateBooleanDataType()
    {
        int dataTypeId = getDataTypeId(BOOLEAN_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = createBooleanDataType();
            commitDataType(BOOLEAN_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    protected int createBooleanDataType()
    {
        return h5.createDataTypeEnum(new String[]
            { "FALSE", "TRUE" }, fileRegistry);
    }

    protected int openOrCreateTypeVariantDataType()
    {
        int dataTypeId = getDataTypeId(TYPE_VARIANT_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = createTypeVariantDataType();
        }
        return dataTypeId;
    }

    protected int createTypeVariantDataType()
    {
        final HDF5DataTypeVariant[] typeVariants = HDF5DataTypeVariant.values();
        final String[] typeVariantNames = new String[typeVariants.length];
        for (int i = 0; i < typeVariants.length; ++i)
        {
            typeVariantNames[i] = typeVariants[i].name();
        }
        return h5.createDataTypeEnum(typeVariantNames, fileRegistry);
    }

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    public void close()
    {
        fileRegistry.cleanUp(false);
        closed = true;
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    /**
     * Returns the link information for the given <var>objectPath</var>. If you want to ensure that
     * the link given by <var>objectPath</var> exists, call
     * {@link HDF5LinkInformation#checkExists()}.
     */
    public HDF5LinkInformation getLinkInformation(final String objectPath)
    {
        checkOpen();
        return h5.getLinkInfo(fileId, objectPath, false);
    }

    /**
     * Returns the type of the given <var>objectPath</var>.
     */
    public HDF5ObjectType getObjectType(final String objectPath)
    {
        checkOpen();
        return h5.getTypeInfo(fileId, objectPath, false);
    }

    /**
     * Returns <code>true</code>, if <var>objectPath</var> exists and <code>false</code> otherwise.
     */
    public boolean exists(final String objectPath)
    {
        checkOpen();
        if ("/".equals(objectPath))
        {
            return true;
        }
        return h5.exists(fileId, objectPath);
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

        checkOpen();
        final ICallableWithCleanUp<String> dataTypeNameCallable =
                new ICallableWithCleanUp<String>()
                    {
                        public String call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                            final int dataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                            return h5.tryGetDataTypePath(dataTypeId);
                        }
                    };
        return runner.call(dataTypeNameCallable);
    }

    /**
     * Returns the path of the data <var>type</var>, or <code>null</code>, if <var>type</var> is not
     * a named data type.
     */
    public String tryGetDataTypePath(HDF5DataType type)
    {
        assert type != null;

        checkOpen();
        type.check(fileId);
        return h5.tryGetDataTypePath(type.getStorageTypeId());
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
        checkOpen();
        return removeInternalNames(getAllAttributeNames(objectPath));
    }

    /**
     * Removes all internal names from the list <var>names</var>.
     * 
     * @return The list <var>names</var>.
     */
    private List<String> removeInternalNames(final List<String> names)
    {
        for (Iterator<String> iterator = names.iterator(); iterator.hasNext(); /**/)
        {
            final String memberName = iterator.next();
            if (isInternalName(memberName))
            {
                iterator.remove();
            }
        }
        return names;
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

        checkOpen();
        final ICallableWithCleanUp<List<String>> attributeNameReaderRunnable =
                new ICallableWithCleanUp<List<String>>()
                    {
                        public List<String> call(ICleanUpRegistry registry)
                        {
                            final int objectId = h5.openObject(fileId, objectPath, registry);
                            return h5.getAttributeNames(objectId, registry);
                        }
                    };
        return runner.call(attributeNameReaderRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<HDF5DataTypeInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataTypeInformation>()
                    {
                        public HDF5DataTypeInformation call(ICleanUpRegistry registry)
                        {
                            try
                            {
                                final int objectId = h5.openObject(fileId, dataSetPath, registry);
                                final int attributeId =
                                        h5.openAttribute(objectId, attributeName, registry);
                                final int dataTypeId =
                                        h5.getDataTypeForAttribute(attributeId, registry);
                                return getDataTypeInformation(dataTypeId);
                            } catch (RuntimeException ex)
                            {
                                throw ex;
                            }
                        }
                    };
        return runner.call(informationDeterminationRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<HDF5DataSetInformation> informationDeterminationRunnable =
                new ICallableWithCleanUp<HDF5DataSetInformation>()
                    {
                        public HDF5DataSetInformation call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, dataSetPath, registry);
                            final int dataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                            final HDF5DataTypeInformation dataTypeInfo =
                                    getDataTypeInformation(dataTypeId);
                            final HDF5DataSetInformation dataSetInfo =
                                    new HDF5DataSetInformation(dataTypeInfo, tryGetTypeVariant(
                                            dataSetId, registry));
                            // Is it a variable-length string?
                            final boolean vlString =
                                    (dataTypeInfo.getDataClass() == HDF5DataClass.STRING && h5
                                            .isVariableLengthString(dataTypeId));
                            if (vlString)
                            {
                                dataTypeInfo.setElementSize(1);

                                dataSetInfo.setDimensions(new long[]
                                    { H5T_VARIABLE });
                                dataSetInfo.setMaxDimensions(new long[]
                                    { H5T_VARIABLE });
                            } else
                            {
                                h5.fillDataDimensions(dataSetId, false, dataSetInfo);
                            }
                            return dataSetInfo;
                        }
                    };
        return runner.call(informationDeterminationRunnable);
    }

    private HDF5DataTypeInformation getDataTypeInformation(final int dataTypeId)
    {
        return new HDF5DataTypeInformation(getDataClassForDataType(dataTypeId), h5
                .getSize(dataTypeId));
    }

    private HDF5DataClass getDataClassForDataType(final int dataTypeId)
    {
        HDF5DataClass dataClass = classIdToDataClass(h5.getClassType(dataTypeId));
        // Is it a boolean?
        if (dataClass == HDF5DataClass.ENUM && h5.dataTypesAreEqual(dataTypeId, booleanDataTypeId))
        {
            dataClass = HDF5DataClass.BOOLEAN;
        }
        return dataClass;
    }

    private HDF5DataClass classIdToDataClass(final int classId)
    {
        if (H5T_BITFIELD == classId)
        {
            return HDF5DataClass.BITFIELD;
        } else if (H5T_INTEGER == classId)
        {
            return HDF5DataClass.INTEGER;
        } else if (H5T_FLOAT == classId)
        {
            return HDF5DataClass.FLOAT;
        } else if (H5T_STRING == classId)
        {
            return HDF5DataClass.STRING;
        } else if (H5T_OPAQUE == classId)
        {
            return HDF5DataClass.OPAQUE;
        } else if (H5T_ENUM == classId)
        {
            return HDF5DataClass.ENUM;
        } else if (H5T_COMPOUND == classId)
        {
            return HDF5DataClass.COMPOUND;
        } else
        {
            return HDF5DataClass.OTHER;
        }
    }

    private HDF5DataTypeVariant tryGetTypeVariant(final int dataSetId, ICleanUpRegistry registry)
    {
        final Integer typeVariantOrdinal = tryGetAttributeTypeVariant(dataSetId, registry);
        final HDF5DataTypeVariant typeVariantOrNull;
        if (typeVariantOrdinal != null && typeVariantOrdinal < HDF5DataTypeVariant.values().length)
        {
            typeVariantOrNull = HDF5DataTypeVariant.values()[typeVariantOrdinal];
        } else
        {
            typeVariantOrNull = null;
        }
        return typeVariantOrNull;
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
        checkOpen();
        return removeInternalNames(getAllGroupMembers(groupPath));
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
        checkOpen();
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
    public List<String> getGroupMemberPaths(final String groupPath)
    {
        checkOpen();
        final String superGroupName = (groupPath.equals("/") ? "/" : groupPath + "/");
        final List<String> memberNames = getGroupMembers(groupPath);
        for (int i = 0; i < memberNames.size(); ++i)
        {
            memberNames.set(i, superGroupName + memberNames.get(i));
        }
        return memberNames;
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
        checkOpen();
        if (readLinkTargets)
        {
            return h5.getGroupMemberLinkInfo(fileId, groupPath, false);
        } else
        {
            return h5.getGroupMemberTypeInfo(fileId, groupPath, false);
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
        checkOpen();
        if (readLinkTargets)
        {
            return h5.getGroupMemberLinkInfo(fileId, groupPath, true);
        } else
        {
            return h5.getGroupMemberTypeInfo(fileId, groupPath, true);
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
        checkOpen();
        final ICallableWithCleanUp<String> readTagCallable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int dataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                    return h5.getOpaqueTag(dataTypeId);
                }
            };
        return runner.call(readTagCallable);
    }

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Use this method only when
     * you know that the type exists.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     */
    public HDF5EnumerationType getEnumType(final String name)
    {
        checkOpen();
        final String dataTypePath = createDataTypePath(ENUM_PREFIX, name);
        final int storageDataTypeId = getDataTypeId(dataTypePath);
        final int nativeDataTypeId = h5.getNativeDataType(storageDataTypeId, fileRegistry);
        final String[] values = h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(fileId, storageDataTypeId, nativeDataTypeId, name, values);
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
        checkOpen();
        final HDF5EnumerationType dataType = getEnumType(name);
        checkEnumValues(dataType.getStorageTypeId(), values, name);
        return dataType;
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
        checkOpen();
        final HDF5EnumerationType dataType = getEnumType(name);
        if (check)
        {
            checkEnumValues(dataType.getStorageTypeId(), values, name);
        }
        return dataType;
    }

    protected void checkEnumValues(int dataTypeId, final String[] values, final String nameOrNull)
    {
        final String[] valuesStored = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
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
            final String path = h5.tryGetDataTypePath(dataTypeId);
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
        checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationType> readEnumTypeCallable =
                new ICallableWithCleanUp<HDF5EnumerationType>()
                    {
                        public HDF5EnumerationType call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, dataSetPath, registry);
                            return getEnumTypeForDataSetId(dataSetId);
                        }
                    };
        return runner.call(readEnumTypeCallable);
    }

    private HDF5EnumerationType getEnumTypeForDataSetId(final int objectId)
    {
        final int storageDataTypeId = h5.getDataTypeForDataSet(objectId, fileRegistry);
        final int nativeDataTypeId = h5.getNativeDataType(storageDataTypeId, fileRegistry);
        final String[] values = h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(fileId, storageDataTypeId, nativeDataTypeId, null, values);
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

        checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    return h5.existsAttribute(objectId, attributeName);
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    final int dataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
                    final boolean isString = (h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new IllegalArgumentException("Attribute " + attributeName
                                + " of object " + objectPath + " needs to be a String.");
                    }
                    final int size = h5.getSize(dataTypeId);
                    final int stringDataTypeId = h5.createDataTypeString(size, registry);
                    byte[] data = new byte[size];
                    h5.readAttribute(attributeId, stringDataTypeId, data);
                    int termIdx;
                    for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
                    {
                    }
                    return new String(data, 0, termIdx);
                }
            };
        return runner.call(readRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    final int nativeDataTypeId =
                            h5.getNativeDataTypeForAttribute(attributeId, registry);
                    byte[] data = new byte[1];
                    h5.readAttribute(attributeId, nativeDataTypeId, data);
                    final Boolean value = h5.tryGetBooleanValue(nativeDataTypeId, data[0]);
                    if (value == null)
                    {
                        throw new HDF5JavaException("Attribute " + attributeName + " of path "
                                + objectPath + " needs to be a Boolean.");
                    }
                    return value;
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<String> readRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    final int nativeDataTypeId =
                            h5.getNativeDataTypeForAttribute(attributeId, registry);
                    int[] data = new int[1];
                    h5.readAttribute(attributeId, nativeDataTypeId, data);
                    final String value =
                            h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId, data[0]);
                    if (value == null)
                    {
                        throw new HDF5JavaException("Attribute " + attributeName + " of path "
                                + objectPath + " needs to be an Enumeration.");
                    }
                    return value;
                }
            };
        return runner.call(readRunnable);
    }

    /**
     * Returns the ordinal for the type variant of <var>objectPath</var>, or <code>null</code>, if
     * no type variant is defined for this <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The ordinal of the type variant or <code>null</code>.
     */
    private Integer tryGetAttributeTypeVariant(final String objectPath) throws HDF5JavaException
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<Integer> readRunnable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    return tryGetAttributeTypeVariant(objectId, registry);
                }
            };

        return runner.call(readRunnable);
    }

    /**
     * Returns the ordinal for the type variant of <var>objectPath</var>, or <code>null</code>, if
     * no type variant is defined for this <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The ordinal of the type variant or <code>null</code>.
     */
    private Integer tryGetAttributeTypeVariant(final int objectId, ICleanUpRegistry registry)
    {
        checkOpen();
        if (h5.existsAttribute(objectId, TYPE_VARIANT_ATTRIBUTE) == false)
        {
            return null;
        }
        final int attributeId = h5.openAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, registry);
        final int dataTypeId = h5.getDataTypeForAttribute(attributeId, registry);
        final int[] data = new int[1];
        h5.readAttribute(attributeId, dataTypeId, data);
        return data[0];
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

        checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int objectId = h5.openObject(fileId, objectPath, registry);
                            final int attributeId =
                                    h5.openAttribute(objectId, attributeName, registry);
                            final HDF5EnumerationType enumType =
                                    getEnumTypeForAttributeId(attributeId);
                            final int[] data = new int[1];
                            h5.readAttribute(attributeId, enumType.getStorageTypeId(), data);
                            return new HDF5EnumerationValue(enumType, data[0]);
                        }
                    };

        return runner.call(readRunnable);
    }

    private HDF5EnumerationType getEnumTypeForAttributeId(final int objectId)
    {
        final int storageDataTypeId = h5.getDataTypeForAttribute(objectId, fileRegistry);
        final int nativeDataTypeId = h5.getNativeDataType(storageDataTypeId, fileRegistry);
        final String[] values = h5.getNamesForEnumOrCompoundMembers(storageDataTypeId);
        return new HDF5EnumerationType(fileId, storageDataTypeId, nativeDataTypeId, null, values);
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

        checkOpen();
        final ICallableWithCleanUp<Integer> writeRunnable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    int[] data = new int[1];
                    h5.readAttribute(attributeId, H5T_NATIVE_INT32, data);
                    return data[0];
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<Long> writeRunnable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    long[] data = new long[1];
                    h5.readAttribute(attributeId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<Float> writeRunnable = new ICallableWithCleanUp<Float>()
            {
                public Float call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    float[] data = new float[1];
                    h5.readAttribute(attributeId, H5T_NATIVE_FLOAT, data);
                    return data[0];
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<Double> writeRunnable = new ICallableWithCleanUp<Double>()
            {
                public Double call(ICleanUpRegistry registry)
                {
                    final int objectId = h5.openObject(fileId, objectPath, registry);
                    final int attributeId = h5.openAttribute(objectId, attributeName, registry);
                    double[] data = new double[1];
                    h5.readAttribute(attributeId, H5T_NATIVE_DOUBLE, data);
                    return data[0];
                }
            };
        return runner.call(writeRunnable);
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
        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, -1, byte.class, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code>
     *            returned).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data block read from the data set.
     */
    public byte[] readAsByteArrayBlock(final String objectPath, final int blockSize,
            final int blockNumber)
    {
        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, -1, byte.class, blockSize,
                            blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>byte[]</code>
     *            returned).
     * @param offset The offset of the block to read (starting with 0).
     * @return The data block read from the data set.
     */
    public byte[] readAsByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final int offset)
    {
        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, -1, byte.class, blockSize,
                            offset, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Boolean> writeRunnable = new ICallableWithCleanUp<Boolean>()
            {
                public Boolean call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int nativeDataTypeId =
                            h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final byte[] data = new byte[1];
                    h5.readDataSet(dataSetId, nativeDataTypeId, data);
                    final Boolean value = h5.tryGetBooleanValue(nativeDataTypeId, data[0]);
                    if (value == null)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a Boolean.");
                    }
                    return value;
                }
            };
        return runner.call(writeRunnable);
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
        checkOpen();
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
                    return (long[]) primReadArrayRank1(objectPath, H5T_NATIVE_B64, long.class, -1,
                            -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Byte> readCallable = new ICallableWithCleanUp<Byte>()
            {
                public Byte call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final byte[] data = new byte[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_INT8, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT8, byte.class, -1,
                            -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT8, byte.class,
                            blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<byte[]> readCallable = new ICallableWithCleanUp<byte[]>()
            {
                public byte[] call(ICleanUpRegistry registry)
                {
                    return (byte[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT8, byte.class,
                            blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>byte</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public byte[][] readByteMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<byte[][]> readCallable = new ICallableWithCleanUp<byte[][]>()
            {
                public byte[][] call(ICleanUpRegistry registry)
                {
                    return (byte[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT8, byte.class,
                            -1, -1, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public byte[][] readByteMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<byte[][]> readCallable = new ICallableWithCleanUp<byte[][]>()
            {
                public byte[][] call(ICleanUpRegistry registry)
                {
                    return (byte[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT8, byte.class,
                            blockSizeX, blockSizeY, blockNumberX * blockSizeX, blockNumberY
                                    * blockSizeY, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public byte[][] readByteMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<byte[][]> readCallable = new ICallableWithCleanUp<byte[][]>()
            {
                public byte[][] call(ICleanUpRegistry registry)
                {
                    return (byte[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT8, byte.class,
                            blockSizeX, blockSizeY, offsetX, offsetY, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final byte[] data =
                                    (byte[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT8,
                                            byte.class, null, null, dimensionsContainer, registry);
                            return new MDByteArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final byte[] dataBlock =
                                    (byte[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT8,
                                            byte.class, blockDimensions, offset, null, registry);
                            return new MDByteArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDByteArray> readCallable =
                new ICallableWithCleanUp<MDByteArray>()
                    {
                        public MDByteArray call(ICleanUpRegistry registry)
                        {
                            final byte[] dataBlock =
                                    (byte[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT8,
                                            byte.class, blockDimensions, offset, null, registry);
                            return new MDByteArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Short> readCallable = new ICallableWithCleanUp<Short>()
            {
                public Short call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final short[] data = new short[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_INT16, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    return (short[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT16, short.class,
                            -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    return (short[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT16, short.class,
                            blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<short[]> readCallable = new ICallableWithCleanUp<short[]>()
            {
                public short[] call(ICleanUpRegistry registry)
                {
                    return (short[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT16, short.class,
                            blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>short</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public short[][] readShortMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<short[][]> readCallable = new ICallableWithCleanUp<short[][]>()
            {
                public short[][] call(ICleanUpRegistry registry)
                {
                    return (short[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT16,
                            short.class, -1, -1, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public short[][] readShortMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<short[][]> readCallable = new ICallableWithCleanUp<short[][]>()
            {
                public short[][] call(ICleanUpRegistry registry)
                {
                    return (short[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT16,
                            short.class, blockSizeX, blockSizeY, blockNumberX * blockSizeX,
                            blockNumberY * blockSizeY, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public short[][] readShortMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<short[][]> readCallable = new ICallableWithCleanUp<short[][]>()
            {
                public short[][] call(ICleanUpRegistry registry)
                {
                    return (short[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT16,
                            short.class, blockSizeX, blockSizeY, offsetX, offsetY, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDShortArray> readCallable =
                new ICallableWithCleanUp<MDShortArray>()
                    {
                        public MDShortArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final short[] data =
                                    (short[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT16,
                                            short.class, null, null, dimensionsContainer, registry);
                            return new MDShortArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final short[] dataBlock =
                                    (short[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT16,
                                            short.class, blockDimensions, offset, null, registry);
                            return new MDShortArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDShortArray> readCallable =
                new ICallableWithCleanUp<MDShortArray>()
                    {
                        public MDShortArray call(ICleanUpRegistry registry)
                        {
                            final short[] dataBlock =
                                    (short[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT16,
                                            short.class, blockDimensions, offset, null, registry);
                            return new MDShortArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Integer> readCallable = new ICallableWithCleanUp<Integer>()
            {
                public Integer call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int[] data = new int[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_INT32, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    return (int[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT32, int.class, -1,
                            -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    return (int[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT32, int.class,
                            blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<int[]> readCallable = new ICallableWithCleanUp<int[]>()
            {
                public int[] call(ICleanUpRegistry registry)
                {
                    return (int[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT32, int.class,
                            blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public int[][] readIntMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<int[][]> readCallable = new ICallableWithCleanUp<int[][]>()
            {
                public int[][] call(ICleanUpRegistry registry)
                {
                    return (int[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT32, int.class,
                            -1, -1, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public int[][] readIntMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<int[][]> readCallable = new ICallableWithCleanUp<int[][]>()
            {
                public int[][] call(ICleanUpRegistry registry)
                {
                    return (int[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT32, int.class,
                            blockSizeX, blockSizeY, blockNumberX * blockSizeX, blockNumberY
                                    * blockSizeY, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public int[][] readIntMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<int[][]> readCallable = new ICallableWithCleanUp<int[][]>()
            {
                public int[][] call(ICleanUpRegistry registry)
                {
                    return (int[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT32, int.class,
                            blockSizeX, blockSizeY, offsetX, offsetY, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final int[] data =
                                    (int[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT32,
                                            int.class, null, null, dimensionsContainer, registry);
                            return new MDIntArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final int[] dataBlock =
                                    (int[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT32,
                                            int.class, blockDimensions, offset, null, registry);
                            return new MDIntArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDIntArray> readCallable =
                new ICallableWithCleanUp<MDIntArray>()
                    {
                        public MDIntArray call(ICleanUpRegistry registry)
                        {
                            final int[] dataBlock =
                                    (int[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT32,
                                            int.class, blockDimensions, offset, null, registry);
                            return new MDIntArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Long> readCallable = new ICallableWithCleanUp<Long>()
            {
                public Long call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final long[] data = new long[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_INT64, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    return (long[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT64, long.class,
                            -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    return (long[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT64, long.class,
                            blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<long[]> readCallable = new ICallableWithCleanUp<long[]>()
            {
                public long[] call(ICleanUpRegistry registry)
                {
                    return (long[]) primReadArrayRank1(objectPath, H5T_NATIVE_INT64, long.class,
                            blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>long</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public long[][] readLongMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<long[][]> readCallable = new ICallableWithCleanUp<long[][]>()
            {
                public long[][] call(ICleanUpRegistry registry)
                {
                    return (long[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT64, long.class,
                            -1, -1, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public long[][] readLongMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<long[][]> readCallable = new ICallableWithCleanUp<long[][]>()
            {
                public long[][] call(ICleanUpRegistry registry)
                {
                    return (long[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT64, long.class,
                            blockSizeX, blockSizeY, blockNumberX * blockSizeX, blockNumberY
                                    * blockSizeY, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public long[][] readLongMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<long[][]> readCallable = new ICallableWithCleanUp<long[][]>()
            {
                public long[][] call(ICleanUpRegistry registry)
                {
                    return (long[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_INT64, long.class,
                            blockSizeX, blockSizeY, offsetX, offsetY, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final long[] data =
                                    (long[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT64,
                                            long.class, null, null, dimensionsContainer, registry);
                            return new MDLongArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final long[] dataBlock =
                                    (long[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT64,
                                            long.class, blockDimensions, offset, null, registry);
                            return new MDLongArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDLongArray> readCallable =
                new ICallableWithCleanUp<MDLongArray>()
                    {
                        public MDLongArray call(ICleanUpRegistry registry)
                        {
                            final long[] dataBlock =
                                    (long[]) primReadArrayRankN(objectPath, H5T_NATIVE_INT64,
                                            long.class, blockDimensions, offset, null, registry);
                            return new MDLongArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Float> readCallable = new ICallableWithCleanUp<Float>()
            {
                public Float call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final float[] data = new float[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_FLOAT, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    return (float[]) primReadArrayRank1(objectPath, H5T_NATIVE_FLOAT, float.class,
                            -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    return (float[]) primReadArrayRank1(objectPath, H5T_NATIVE_FLOAT, float.class,
                            blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<float[]> readCallable = new ICallableWithCleanUp<float[]>()
            {
                public float[] call(ICleanUpRegistry registry)
                {
                    return (float[]) primReadArrayRank1(objectPath, H5T_NATIVE_FLOAT, float.class,
                            blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>float</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public float[][] readFloatMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<float[][]> readCallable = new ICallableWithCleanUp<float[][]>()
            {
                public float[][] call(ICleanUpRegistry registry)
                {
                    return (float[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_FLOAT,
                            float.class, -1, -1, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public float[][] readFloatMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<float[][]> readCallable = new ICallableWithCleanUp<float[][]>()
            {
                public float[][] call(ICleanUpRegistry registry)
                {
                    return (float[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_FLOAT,
                            float.class, blockSizeX, blockSizeY, blockNumberX * blockSizeX,
                            blockNumberY * blockSizeY, registry);
                }
            };
        return runner.call(readCallable);
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
     */
    public float[][] readFloatMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<float[][]> readCallable = new ICallableWithCleanUp<float[][]>()
            {
                public float[][] call(ICleanUpRegistry registry)
                {
                    return (float[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_FLOAT,
                            float.class, blockSizeX, blockSizeY, offsetX, offsetY, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDFloatArray> readCallable =
                new ICallableWithCleanUp<MDFloatArray>()
                    {
                        public MDFloatArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final float[] data =
                                    (float[]) primReadArrayRankN(objectPath, H5T_NATIVE_FLOAT,
                                            float.class, null, null, dimensionsContainer, registry);
                            return new MDFloatArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final float[] dataBlock =
                                    (float[]) primReadArrayRankN(objectPath, H5T_NATIVE_FLOAT,
                                            float.class, blockDimensions, offset, null, registry);
                            return new MDFloatArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDFloatArray> readCallable =
                new ICallableWithCleanUp<MDFloatArray>()
                    {
                        public MDFloatArray call(ICleanUpRegistry registry)
                        {
                            final float[] dataBlock =
                                    (float[]) primReadArrayRankN(objectPath, H5T_NATIVE_FLOAT,
                                            float.class, blockDimensions, offset, null, registry);
                            return new MDFloatArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<Double> readCallable = new ICallableWithCleanUp<Double>()
            {
                public Double call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final double[] data = new double[1];
                    h5.readDataSet(dataSetId, H5T_NATIVE_DOUBLE, data);
                    return data[0];
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    return (double[]) primReadArrayRank1(objectPath, H5T_NATIVE_DOUBLE,
                            double.class, -1, -1, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    return (double[]) primReadArrayRank1(objectPath, H5T_NATIVE_DOUBLE,
                            double.class, blockSize, blockNumber * blockSize, registry);
                }
            };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<double[]> readCallable = new ICallableWithCleanUp<double[]>()
            {
                public double[] call(ICleanUpRegistry registry)
                {
                    return (double[]) primReadArrayRank1(objectPath, H5T_NATIVE_DOUBLE,
                            double.class, blockSize, offset, registry);
                }
            };
        return runner.call(readCallable);
    }

    /**
     * Reads a <code>double</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public double[][] readDoubleMatrix(final String objectPath)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<double[][]> readCallable =
                new ICallableWithCleanUp<double[][]>()
                    {
                        public double[][] call(ICleanUpRegistry registry)
                        {
                            return (double[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_DOUBLE,
                                    double.class, -1, -1, -1, -1, registry);
                        }
                    };
        return runner.call(readCallable);
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
     */
    public double[][] readDoubleMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<double[][]> readCallable =
                new ICallableWithCleanUp<double[][]>()
                    {
                        public double[][] call(ICleanUpRegistry registry)
                        {
                            return (double[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_DOUBLE,
                                    double.class, blockSizeX, blockSizeY,
                                    blockNumberX * blockSizeX, blockNumberY * blockSizeY, registry);
                        }
                    };
        return runner.call(readCallable);
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
     */
    public double[][] readDoubleMatrixBlockWithOffset(final String objectPath,
            final int blockSizeX, final int blockSizeY, final long offsetX, final long offsetY)
    {
        assert objectPath != null;

        checkOpen();
        final ICallableWithCleanUp<double[][]> readCallable =
                new ICallableWithCleanUp<double[][]>()
                    {
                        public double[][] call(ICleanUpRegistry registry)
                        {
                            return (double[][]) primReadMatrixRank2(objectPath, H5T_NATIVE_DOUBLE,
                                    double.class, blockSizeX, blockSizeY, offsetX, offsetY,
                                    registry);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDDoubleArray> readCallable =
                new ICallableWithCleanUp<MDDoubleArray>()
                    {
                        public MDDoubleArray call(ICleanUpRegistry registry)
                        {
                            final long[][] dimensionsContainer = new long[1][];
                            final double[] data =
                                    (double[]) primReadArrayRankN(objectPath, H5T_NATIVE_DOUBLE,
                                            double.class, null, null, dimensionsContainer, registry);
                            return new MDDoubleArray(data, dimensionsContainer[0]);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
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
                            final double[] dataBlock =
                                    (double[]) primReadArrayRankN(objectPath, H5T_NATIVE_DOUBLE,
                                            double.class, blockDimensions, offset, null, registry);
                            return new MDDoubleArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
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

        checkOpen();
        final ICallableWithCleanUp<MDDoubleArray> readCallable =
                new ICallableWithCleanUp<MDDoubleArray>()
                    {
                        public MDDoubleArray call(ICleanUpRegistry registry)
                        {
                            final double[] dataBlock =
                                    (double[]) primReadArrayRankN(objectPath, H5T_NATIVE_DOUBLE,
                                            double.class, blockDimensions, offset, null, registry);
                            return new MDDoubleArray(dataBlock, blockDimensions);
                        }
                    };
        return runner.call(readCallable);
    }

    // ------------------------------------------------------------------------------
    // GENERATED CODE SECTION - END
    // ------------------------------------------------------------------------------

    //
    // Date
    //

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
        checkOpen();
        if (isTimeStamp(objectPath) == false)
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time stamp.");
        }
        return readLong(objectPath);
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
    public long[] readTimeStampArray(final String objectPath) throws HDF5JavaException
    {
        checkOpen();
        if (isTimeStamp(objectPath) == false)
        {
            throw new HDF5JavaException("Data set '" + objectPath + "' is not a time stamp array.");
        }
        return readLongArray(objectPath);
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

    private Date[] timeStampsToDates(final long[] timeStampArray)
    {
        final Date[] dateArray = new Date[timeStampArray.length];
        for (int i = 0; i < dateArray.length; ++i)
        {
            dateArray[i] = new Date(timeStampArray[i]);
        }
        return dateArray;
    }

    private boolean isTimeStamp(final String objectPath)
    {
        final Integer typeVariantOrdinalOrNull = tryGetAttributeTypeVariant(objectPath);
        return typeVariantOrdinalOrNull != null
                && typeVariantOrdinalOrNull == HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH
                        .ordinal();
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

        checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int dataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final boolean isString = (h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a String.");
                    }
                    if (h5.isVariableLengthString(dataTypeId))
                    {
                        String[] data = new String[1];
                        h5.readDataSetVL(dataSetId, dataTypeId, data);
                        return data[0];
                    } else
                    {
                        final int size = h5.getSize(dataTypeId);
                        byte[] data = new byte[size];
                        h5.readDataSetNonNumeric(dataSetId, dataTypeId, data);
                        int termIdx;
                        for (termIdx = 0; termIdx < size && data[termIdx] != 0; ++termIdx)
                        {
                        }
                        return new String(data, 0, termIdx);
                    }
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final long[] dimensions = h5.getDataDimensions(dataSetId);
                    final String[] data = new String[getOneDimensionalArraySize(dimensions)];
                    final int dataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final boolean isString = (h5.getClassType(dataTypeId) == H5T_STRING);
                    if (isString == false)
                    {
                        throw new HDF5JavaException(objectPath + " needs to be a String.");
                    }
                    h5.readDataSetNonNumeric(dataSetId, dataTypeId, data);
                    return data;
                }
            };
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<String> writeRunnable = new ICallableWithCleanUp<String>()
            {
                public String call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int nativeDataTypeId =
                            h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final int size = h5.getSize(nativeDataTypeId);
                    final String value;
                    switch (size)
                    {
                        case 1:
                        {
                            final byte[] data = new byte[1];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                            data[0]);
                            break;
                        }
                        case 2:
                        {
                            final short[] data = new short[1];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                            data[0]);
                            break;
                        }
                        case 4:
                        {
                            final int[] data = new int[1];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            value =
                                    h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                            data[0]);
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
        return runner.call(writeRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                            final HDF5EnumerationType enumType = getEnumTypeForDataSetId(dataSetId);
                            final int[] data = new int[1];
                            h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
                            return new HDF5EnumerationValue(enumType, data[0]);
                        }
                    };

        return runner.call(readRunnable);
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

        checkOpen();
        enumType.check(fileId);
        final ICallableWithCleanUp<HDF5EnumerationValue> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValue>()
                    {
                        public HDF5EnumerationValue call(ICleanUpRegistry registry)
                        {
                            final int[] data = new int[1];
                            final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                            h5.readDataSet(dataSetId, enumType.getNativeTypeId(), data);
                            return new HDF5EnumerationValue(enumType, data[0]);
                        }
                    };

        return runner.call(readRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<HDF5EnumerationValueArray> readRunnable =
                new ICallableWithCleanUp<HDF5EnumerationValueArray>()
                    {
                        public HDF5EnumerationValueArray call(ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                            final long[] dimensions = h5.getDataDimensions(dataSetId);
                            final HDF5EnumerationType actualEnumType =
                                    (enumType == null) ? getEnumTypeForDataSetId(dataSetId)
                                            : enumType;
                            final Object data =
                                    actualEnumType.createArray(HDF5Utils
                                            .getOneDimensionalArraySize(dimensions));
                            h5.readDataSet(dataSetId, actualEnumType.getNativeTypeId(), data);
                            return new HDF5EnumerationValueArray(actualEnumType, data);
                        }
                    };

        return runner.call(readRunnable);
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

        checkOpen();
        final ICallableWithCleanUp<String[]> writeRunnable = new ICallableWithCleanUp<String[]>()
            {
                public String[] call(ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final long[] dimensions = h5.getDataDimensions(dataSetId);
                    final int vectorLength = getOneDimensionalArraySize(dimensions);
                    final int nativeDataTypeId =
                            h5.getNativeDataTypeForDataSet(dataSetId, registry);
                    final boolean isEnum = (h5.getClassType(nativeDataTypeId) == H5T_ENUM);
                    if (isEnum == false)
                    {
                        throw new HDF5JavaException(objectPath + " is not an enum.");
                    }
                    final int size = h5.getSize(nativeDataTypeId);

                    final String[] value = new String[vectorLength];
                    switch (size)
                    {
                        case 1:
                        {
                            final byte[] data = new byte[vectorLength];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                                data[i]);
                            }
                            break;
                        }
                        case 2:
                        {
                            final short[] data = new short[vectorLength];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                                data[i]);
                            }
                            break;
                        }
                        case 4:
                        {
                            final int[] data = new int[vectorLength];
                            h5.readDataSet(dataSetId, nativeDataTypeId, data);
                            for (int i = 0; i < data.length; ++i)
                            {
                                value[i] =
                                        h5.getNameForEnumOrCompoundMemberIndex(nativeDataTypeId,
                                                data[i]);
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
        return runner.call(writeRunnable);
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
        checkOpen();
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> writeRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            final String dataTypePath =
                                    HDF5Utils.createDataTypePath(HDF5Utils.COMPOUND_PREFIX,
                                            dataTypeName);
                            final int compoundDataTypeId =
                                    h5.openDataType(fileId, dataTypePath, registry);
                            return getCompoundMemberInformation(compoundDataTypeId);
                        }
                    };
        return runner.call(writeRunnable);
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
        checkOpen();
        final ICallableWithCleanUp<HDF5CompoundMemberInformation[]> writeRunnable =
                new ICallableWithCleanUp<HDF5CompoundMemberInformation[]>()
                    {
                        public HDF5CompoundMemberInformation[] call(final ICleanUpRegistry registry)
                        {
                            final int dataSetId = h5.openDataSet(fileId, dataSetPath, registry);
                            final int compoundDataTypeId =
                                    h5.getDataTypeForDataSet(dataSetId, registry);
                            if (h5.getClassType(compoundDataTypeId) != H5T_COMPOUND)
                            {
                                throw new HDF5JavaException("Data set '" + dataSetPath
                                        + "' is not of compound type.");
                            }
                            return getCompoundMemberInformation(compoundDataTypeId);
                        }
                    };
        return runner.call(writeRunnable);
    }

    private HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final int compoundDataTypeId)
    {
        final String[] memberNames = h5.getNamesForEnumOrCompoundMembers(compoundDataTypeId);
        final HDF5CompoundMemberInformation[] memberInfo =
                new HDF5CompoundMemberInformation[memberNames.length];
        for (int i = 0; i < memberInfo.length; ++i)
        {
            final int dataTypeId = h5.getDataTypeForIndex(compoundDataTypeId, i);
            memberInfo[i] =
                    new HDF5CompoundMemberInformation(memberNames[i], new HDF5DataTypeInformation(
                            getDataClassForDataType(dataTypeId), h5.getSize(dataTypeId)));
        }
        Arrays.sort(memberInfo);
        return memberInfo;
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
        checkOpen();
        final HDF5ValueObjectByteifyer<T> objectArrayifyer =
                createByteifyers(compoundType, members);
        final int storageDataTypeId = createStorageCompoundDataType(objectArrayifyer);
        final int nativeDataTypeId = createNativeCompoundDataType(objectArrayifyer);
        return new HDF5CompoundType<T>(fileId, storageDataTypeId, nativeDataTypeId, name,
                compoundType, objectArrayifyer);
    }

    protected int createStorageCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int storageDataTypeId =
                h5.createDataTypeCompound(objectArrayifyer.getRecordSize(), fileRegistry);
        objectArrayifyer.insertMemberTypes(storageDataTypeId);
        return storageDataTypeId;
    }

    protected int createNativeCompoundDataType(HDF5ValueObjectByteifyer<?> objectArrayifyer)
    {
        final int nativeDataTypeId =
                h5.createDataTypeCompound(objectArrayifyer.getRecordSize(), fileRegistry);
        objectArrayifyer.insertNativeMemberTypes(nativeDataTypeId, h5, fileRegistry);
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
        checkOpen();
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
        checkOpen();
        type.check(fileId);
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
        checkOpen();
        type.check(fileId);
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
        checkOpen();
        type.check(fileId);
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
        checkOpen();
        type.check(fileId);
        return primReadCompoundArray(objectPath, blockSize, offset, type);
    }

    private <T> T primReadCompound(final String objectPath, final int blockSize, final long offset,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        final ICallableWithCleanUp<T> writeRunnable = new ICallableWithCleanUp<T>()
            {
                public T call(final ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int storageDataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    h5.readDataSet(dataSetId, nativeDataTypeId, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, byteArr);
                    return type.getObjectByteifyer().arrayifyScalar(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return runner.call(writeRunnable);
    }

    private <T> T[] primReadCompoundArray(final String objectPath, final int blockSize,
            final long offset, final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        final ICallableWithCleanUp<T[]> writeRunnable = new ICallableWithCleanUp<T[]>()
            {
                public T[] call(final ICleanUpRegistry registry)
                {
                    final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                    final int storageDataTypeId = h5.getDataTypeForDataSet(dataSetId, registry);
                    checkCompoundType(storageDataTypeId, objectPath, registry);
                    final DataSpaceParameters spaceParams =
                            getSpaceParameters(dataSetId, offset, blockSize, registry);
                    final int nativeDataTypeId = type.getNativeTypeId();
                    final byte[] byteArr =
                            new byte[spaceParams.blockSize
                                    * type.getObjectByteifyer().getRecordSize()];
                    h5.readDataSet(dataSetId, nativeDataTypeId, spaceParams.memorySpaceId,
                            spaceParams.dataSpaceId, byteArr);
                    return type.getObjectByteifyer().arrayify(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return runner.call(writeRunnable);
    }

    private void checkCompoundType(final int dataTypeId, final String path,
            final ICleanUpRegistry registry)
    {
        final boolean isCompound = (h5.getClassType(dataTypeId) == H5T_COMPOUND);
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
                                    return booleanDataTypeId;
                                }

                                public int getStringDataTypeId(int maxLength)
                                {
                                    final int typeId =
                                            h5.createDataTypeString(maxLength, fileRegistry);
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
        checkOpen();
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
        checkOpen();
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
        checkOpen();
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
                            final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
                            final int storageDataTypeId =
                                    h5.getDataTypeForDataSet(dataSetId, registry);
                            checkCompoundType(storageDataTypeId, objectPath, registry);
                            final DataSpaceParameters spaceParams =
                                    getSpaceParameters(dataSetId, offsetOrNull, dimensionsOrNull,
                                            registry);
                            final int nativeDataTypeId = type.getNativeTypeId();
                            final byte[] byteArr =
                                    new byte[spaceParams.blockSize
                                            * type.getObjectByteifyer().getRecordSize()];
                            h5.readDataSet(dataSetId, nativeDataTypeId, spaceParams.memorySpaceId,
                                    spaceParams.dataSpaceId, byteArr);
                            return new MDArray<T>(type.getObjectByteifyer().arrayify(
                                    storageDataTypeId, byteArr, type.getCompoundType()),
                                    spaceParams.dimensions);
                        }
                    };
        return runner.call(writeRunnable);
    }

    protected void readNamedDataTypes()
    {
        if (exists(DATATYPE_GROUP) == false)
        {
            return;
        }
        for (String dataTypePath : getGroupMemberPaths(DATATYPE_GROUP))
        {
            final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
            namedDataTypeMap.put(dataTypePath, dataTypeId);
        }
    }

    protected int getDataTypeId(final String dataTypePath)
    {
        final Integer dataTypeIdOrNull = namedDataTypeMap.get(dataTypePath);
        if (dataTypeIdOrNull == null)
        { // Just in case of data types added to other groups than HDF5Utils.DATATYPE_GROUP
            if (exists(dataTypePath))
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

    @SuppressWarnings("unchecked")
    protected <T> Class<T> getComponentType(final T[] value)
    {
        return (Class<T>) value.getClass().getComponentType();
    }

    /**
     * Class to store the parameters of a data space.
     */
    private static class DataSpaceParameters
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

    private DataSpaceParameters getSpaceParameters(final int dataSetId, final long offset,
            final int blockSize, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final int actualBlockSize;
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
            final long maxBlockSize = size - offset;
            if (maxBlockSize <= 0)
            {
                throw new HDF5JavaException("Offset " + offset + " >= Size " + size);
            }
            actualBlockSize = (int) Math.min(blockSize, maxBlockSize);
            final long[] blockShape = new long[]
                { actualBlockSize };
            h5.setHyperslabBlock(dataSpaceId, new long[]
                { offset }, blockShape);
            memorySpaceId = h5.createSimpleDataSpace(blockShape, registry);

        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            dimensions = h5.getDataDimensions(dataSetId);
            actualBlockSize = getOneDimensionalArraySize(dimensions);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, actualBlockSize, dimensions);
    }

    private DataSpaceParameters getSpaceParameters(final int dataSetId, final long[] offset,
            final int[] blockDimensions, ICleanUpRegistry registry)
    {
        final int memorySpaceId;
        final int dataSpaceId;
        final long[] effectiveBlockDimensions;
        if (blockDimensions != null)
        {
            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            final long[] dimensions = h5.getDataSpaceDimensions(dataSpaceId);
            if (dimensions.length != blockDimensions.length)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank "
                        + blockDimensions.length + " (rank=" + dimensions.length + ")");
            }
            effectiveBlockDimensions = MDArray.toLong(blockDimensions);
            h5.setHyperslabBlock(dataSpaceId, offset, effectiveBlockDimensions);
            memorySpaceId = h5.createSimpleDataSpace(effectiveBlockDimensions, registry);
        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            effectiveBlockDimensions = h5.getDataDimensions(dataSetId);
        }
        return new DataSpaceParameters(memorySpaceId, dataSpaceId, MDArray
                .getLength(effectiveBlockDimensions), effectiveBlockDimensions);
    }

    /**
     * Reads an array block (of rank 1) from the data set <var>objectPath</var> and a data type of
     * <var>dataTypeId</var>. The <var>componentType</var> needs to match the <var>dataTypeId</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set. (A one-dimension array of <var>componentType</var>).
     */
    private Object primReadArrayRank1(final String objectPath, final int specifiedDataSetTypeId,
            Class<?> componentType, final int blockSize, final long offset,
            ICleanUpRegistry registry)
    {
        assert objectPath != null;
        assert componentType != null;
        assert registry != null;

        final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
        final DataSpaceParameters spaceParams =
                getSpaceParameters(dataSetId, offset, blockSize, registry);
        int nativeDataTypeId = -1;
        if (specifiedDataSetTypeId < 0)
        {
            nativeDataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
        } else
        {
            nativeDataTypeId = specifiedDataSetTypeId;
        }
        final Object data = Array.newInstance(componentType, spaceParams.blockSize);
        h5.readDataSet(dataSetId, nativeDataTypeId, spaceParams.memorySpaceId,
                spaceParams.dataSpaceId, data);
        return data;
    }

    /**
     * Reads a matrix block (of rank 2) from the data set <var>objectPath</var> and a data type of
     * <var>dataTypeId</var>. The <var>componentType</var> needs to match the <var>dataTypeId</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set. (A one-dimension array of <var>componentType</var>).
     */
    private Object primReadMatrixRank2(final String objectPath, final int specifiedDataSetTypeId,
            Class<?> componentType, final int blockSizeX, final int blockSizeY, final long offsetX,
            final long offsetY, ICleanUpRegistry registry)
    {
        assert objectPath != null;
        assert componentType != null;
        assert registry != null;

        final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
        final int memorySpaceId;
        final int dataSpaceId;
        final long[] actualBlockShape;
        final long[] shape;
        if (blockSizeX > 0 && blockSizeY > 0)
        {
            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            shape = h5.getDataSpaceDimensions(dataSpaceId);
            if (shape.length != 2)
            {
                throw new HDF5JavaException("Data Set must be of rank 2 (rank=" + shape.length
                        + ")");
            }
            final long sizeX = shape[0];
            final long sizeY = shape[1];
            final long maxBlockSizeX = sizeX - offsetX;
            final long maxBlockSizeY = sizeY - offsetY;
            if (maxBlockSizeX <= 0)
            {
                throw new HDF5JavaException("Offset " + offsetX + " >= Size " + sizeX);
            }
            if (maxBlockSizeY <= 0)
            {
                throw new HDF5JavaException("Offset " + offsetY + " >= Size " + sizeY);
            }
            actualBlockShape = new long[]
                { Math.min(blockSizeX, maxBlockSizeX), Math.min(blockSizeY, maxBlockSizeY) };
            h5.setHyperslabBlock(dataSpaceId, new long[]
                { offsetX, offsetY }, actualBlockShape);
            memorySpaceId = h5.createSimpleDataSpace(actualBlockShape, registry);
        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            shape = h5.getDataDimensions(dataSetId);
            if (shape.length != 2)
            {
                throw new HDF5JavaException("Data Set must be of rank 2 (rank=" + shape.length
                        + ")");
            }
            actualBlockShape = shape;
        }
        int nativeDataTypeId = -1;
        if (specifiedDataSetTypeId < 0)
        {
            nativeDataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
        } else
        {
            nativeDataTypeId = specifiedDataSetTypeId;
        }
        final Object data = Array.newInstance(componentType, MDArray.toInt(actualBlockShape));
        h5.readDataSet(dataSetId, nativeDataTypeId, memorySpaceId, dataSpaceId, data);
        return data;
    }

    /**
     * Reads an array block (of rank N) from the data set <var>objectPath</var> and a data type of
     * <var>dataTypeId</var>. The <var>componentType</var> needs to match the <var>dataTypeId</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set. (A one-dimension array of <var>componentType</var>).
     */
    private Object primReadArrayRankN(final String objectPath, final int specifiedDataSetTypeId,
            Class<?> componentType, final int[] blockShapeOrNull, final long[] offset,
            final long[][] shapeContainerOrNull, ICleanUpRegistry registry)
    {
        assert objectPath != null;
        assert componentType != null;
        assert registry != null;

        final int dataSetId = h5.openDataSet(fileId, objectPath, registry);
        final int memorySpaceId;
        final int dataSpaceId;
        final long[] actualBlockShape;
        final long[] shape;
        if (blockShapeOrNull != null)
        {
            assert offset != null;
            assert blockShapeOrNull.length == offset.length;

            dataSpaceId = h5.getDataSpaceForDataSet(dataSetId, registry);
            shape = h5.getDataSpaceDimensions(dataSpaceId);
            if (shape.length != blockShapeOrNull.length)
            {
                throw new HDF5JavaException("Data Set is expected to be of rank "
                        + blockShapeOrNull.length + " (rank=" + shape.length + ")");
            }
            actualBlockShape = new long[blockShapeOrNull.length];
            for (int i = 0; i < offset.length; ++i)
            {
                final long maxBlockSize = shape[i] - offset[i];
                if (maxBlockSize <= 0)
                {
                    throw new HDF5JavaException("Offset " + offset[i] + " >= Size " + shape[i]);
                }
                actualBlockShape[i] = Math.min(blockShapeOrNull[i], maxBlockSize);
            }
            h5.setHyperslabBlock(dataSpaceId, offset, actualBlockShape);
            memorySpaceId = h5.createSimpleDataSpace(actualBlockShape, registry);
        } else
        {
            memorySpaceId = HDF5Constants.H5S_ALL;
            dataSpaceId = HDF5Constants.H5S_ALL;
            shape = h5.getDataDimensions(dataSetId);
            actualBlockShape = shape;
        }
        if (shapeContainerOrNull != null)
        {
            shapeContainerOrNull[0] = shape;
        }
        int nativeDataTypeId = -1;
        if (specifiedDataSetTypeId < 0)
        {
            nativeDataTypeId = h5.getNativeDataTypeForDataSet(dataSetId, registry);
        } else
        {
            nativeDataTypeId = specifiedDataSetTypeId;
        }

        final Object data = Array.newInstance(componentType, MDArray.getLength(actualBlockShape));
        h5.readDataSet(dataSetId, nativeDataTypeId, memorySpaceId, dataSpaceId, data);
        return data;
    }

}
