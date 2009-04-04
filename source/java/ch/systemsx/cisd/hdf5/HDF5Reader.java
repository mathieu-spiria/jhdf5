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
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_BITFIELD;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_COMPOUND;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ENUM;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_INTEGER;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_DOUBLE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_OPAQUE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STRING;

import java.io.File;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.HDFNativeData;
import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.HDF5BaseReader.DataSpaceParameters;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;
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

    public boolean isPerformNumericConversions()
    {
        return baseReader.performNumericConversions;
    }

    public File getFile()
    {
        return baseReader.hdf5File;
    }

    public void close()
    {
        baseReader.close();
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    public HDF5LinkInformation getLinkInformation(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getLinkInfo(baseReader.fileId, objectPath, false);
    }

    public HDF5ObjectInformation getObjectInformation(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getObjectInfo(baseReader.fileId, objectPath, false);
    }

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

    public HDF5ObjectType getObjectType(final String objectPath)
    {
        return getObjectType(objectPath, true);
    }

    public boolean exists(final String objectPath, boolean followLink)
    {
        baseReader.checkOpen();
        if ("/".equals(objectPath))
        {
            return true;
        }
        if (followLink == false)
        {
            // Optimization
            return baseReader.h5.exists(baseReader.fileId, objectPath);
        } else
        {
            return exists(objectPath);
        }
    }

    public boolean exists(final String objectPath)
    {
        baseReader.checkOpen();
        return baseReader.h5.getObjectTypeId(baseReader.fileId, objectPath, false) >= 0;
    }

    public boolean isGroup(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isGroup(getObjectType(objectPath, followLink));
    }

    public boolean isGroup(final String objectPath)
    {
        return HDF5ObjectType.isGroup(getObjectType(objectPath));
    }

    public boolean isDataSet(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isDataSet(getObjectType(objectPath, followLink));
    }

    public boolean isDataSet(final String objectPath)
    {
        return HDF5ObjectType.isDataSet(getObjectType(objectPath));
    }

    public boolean isDataType(final String objectPath, boolean followLink)
    {
        return HDF5ObjectType.isDataType(getObjectType(objectPath, followLink));
    }

    public boolean isDataType(final String objectPath)
    {
        return HDF5ObjectType.isDataType(getObjectType(objectPath));
    }

    public boolean isSoftLink(final String objectPath)
    {
        return HDF5ObjectType.isSoftLink(getObjectType(objectPath, false));
    }

    public boolean isExternalLink(final String objectPath)
    {
        return HDF5ObjectType.isExternalLink(getObjectType(objectPath, false));
    }

    public boolean isSymbolicLink(final String objectPath)
    {
        return HDF5ObjectType.isSymbolicLink(getObjectType(objectPath, false));
    }
    
    public String tryGetSymbolicLinkTarget(final String objectPath)
    {
        return getLinkInformation(objectPath).tryGetSymbolicLinkTarget();
    }

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

    public String tryGetDataTypePath(HDF5DataType type)
    {
        assert type != null;

        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return baseReader.h5.tryGetDataTypePath(type.getStorageTypeId());
    }

    public List<String> getAttributeNames(final String objectPath)
    {
        assert objectPath != null;
        baseReader.checkOpen();
        return removeInternalNames(getAllAttributeNames(objectPath));
    }

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

    public HDF5DataSetInformation getDataSetInformation(final String dataSetPath)
    {
        assert dataSetPath != null;

        baseReader.checkOpen();
        return baseReader.getDataSetInformation(dataSetPath);
    }
    
    public long getSize(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final int objectId = baseReader.h5.getObjectTypeId(baseReader.fileId, objectPath, false);
        if (objectId < 0)
        {
            return -1;
        } else if (objectId == HDF5Constants.H5O_TYPE_DATASET)
        {
            return getDataSetInformation(objectPath).getSize();
        } else
        {
            return 0;
        }
    }

    public long getNumberOfElements(final String objectPath)
    {
        assert objectPath != null;

        baseReader.checkOpen();
        final int objectId = baseReader.h5.getObjectTypeId(baseReader.fileId, objectPath, false);
        if (objectId < 0)
        {
            return -1;
        } else if (objectId == HDF5Constants.H5O_TYPE_DATASET)
        {
            return getDataSetInformation(objectPath).getNumberOfElements();
        } else
        {
            return 0;
        }
    }

    // /////////////////////
    // Group
    // /////////////////////

    public List<String> getGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMembers(groupPath);
    }

    public List<String> getAllGroupMembers(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getAllGroupMembers(groupPath);
    }

    public List<String> getGroupMemberPaths(final String groupPath)
    {
        assert groupPath != null;

        baseReader.checkOpen();
        return baseReader.getGroupMemberPaths(groupPath);
    }

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
                    return baseReader.h5.tryGetOpaqueTag(dataTypeId);
                }
            };
        return baseReader.runner.call(readTagCallable);
    }

    public HDF5OpaqueType tryGetOpaqueType(final String objectPath)
    {
        baseReader.checkOpen();
        final ICallableWithCleanUp<HDF5OpaqueType> readTagCallable =
                new ICallableWithCleanUp<HDF5OpaqueType>()
                    {
                        public HDF5OpaqueType call(ICleanUpRegistry registry)
                        {
                            final int dataSetId =
                                    baseReader.h5.openDataSet(baseReader.fileId, objectPath,
                                            registry);
                            final int dataTypeId =
                                    baseReader.h5.getDataTypeForDataSet(dataSetId,
                                            baseReader.fileRegistry);
                            final String opaqueTagOrNull =
                                    baseReader.h5.tryGetOpaqueTag(dataTypeId);
                            if (opaqueTagOrNull == null)
                            {
                                return null;
                            } else
                            {
                                return new HDF5OpaqueType(baseReader.fileId, dataTypeId,
                                        opaqueTagOrNull);
                            }
                        }
                    };
        return baseReader.runner.call(readTagCallable);
    }

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

    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return getEnumType(name, values, true);
    }

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

    // /////////////////////
    // Attributes
    // /////////////////////

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

    public BitSet readBitField(final String objectPath) throws HDF5DatatypeInterfaceException
    {
        baseReader.checkOpen();
        return BitSetConversionUtils.fromStorageForm(readBitFieldStorageForm(objectPath));
    }

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

    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeStamp();
    }

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

    public Date readDate(final String objectPath) throws HDF5JavaException
    {
        return new Date(readTimeStamp(objectPath));
    }

    public Date[] readDateArray(final String objectPath) throws HDF5JavaException
    {
        final long[] timeStampArray = readTimeStampArray(objectPath);
        return timeStampsToDates(timeStampArray);
    }

    private static Date[] timeStampsToDates(final long[] timeStampArray)
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

    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return typeVariantOrNull != null && typeVariantOrNull.isTimeDuration();
    }

    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException
    {
        final HDF5DataTypeVariant typeVariantOrNull = tryGetTypeVariant(objectPath);
        return (typeVariantOrNull != null) ? typeVariantOrNull.tryGetTimeUnit() : null;
    }

    public long readTimeDuration(final String objectPath) throws HDF5JavaException
    {
        return readTimeDuration(objectPath, HDF5TimeUnit.SECONDS);
    }

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

    public long[] readTimeDurationArray(final String objectPath) throws HDF5JavaException
    {
        return readTimeDurationArray(objectPath, HDF5TimeUnit.SECONDS);
    }

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
                                        baseReader.h5
                                                .readDataSet(dataSetId, H5T_NATIVE_INT16, data);
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
                                        baseReader.h5
                                                .readDataSet(dataSetId, H5T_NATIVE_INT32, data);
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

    public HDF5EnumerationValueArray readEnumArray(final String objectPath)
            throws HDF5JavaException
    {
        return readEnumArray(objectPath, null);
    }

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

    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass)
    {
        return getCompoundMemberInformation(compoundClass.getSimpleName());
    }

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

    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members)
    {
        baseReader.checkOpen();
        return getCompoundType(null, compoundType, members);
    }

    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return readCompound(objectPath, type, null);
    }

    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompound(objectPath, -1, -1, type, inspectorOrNull);
    }

    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException
    {
        return readCompoundArray(objectPath, type, null);
    }

    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, -1, -1, type, inspectorOrNull);
    }

    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber) throws HDF5JavaException
    {
        return readCompoundArrayBlock(objectPath, type, blockSize, blockNumber, null);
    }

    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, blockSize * blockNumber, type,
                inspectorOrNull);
    }

    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset)
            throws HDF5JavaException
    {
        return readCompoundArrayBlockWithOffset(objectPath, type, blockSize, offset, null);
    }

    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        type.check(baseReader.fileId);
        return primReadCompoundArray(objectPath, blockSize, offset, type, inspectorOrNull);
    }

    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return getCompoundArrayNaturalBlocks(objectPath, type, null);
    }

    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        final HDF5DataSetInformation info = getDataSetInformation(objectPath);
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
                                        readCompoundArrayBlockWithOffset(objectPath, type,
                                                blockSize, offset, inspectorOrNull);
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
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
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
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArr);
                    }
                    return type.getObjectByteifyer().arrayifyScalar(storageDataTypeId, byteArr,
                            type.getCompoundType());
                }
            };
        return baseReader.runner.call(writeRunnable);
    }

    private <T> T[] primReadCompoundArray(final String objectPath, final int blockSize,
            final long offset, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
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
                    if (inspectorOrNull != null)
                    {
                        inspectorOrNull.inspect(byteArr);
                    }
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

    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException
    {
        return readCompoundMDArray(objectPath, type, null);
    }

    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, null, null, inspectorOrNull);
    }

    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber)
            throws HDF5JavaException
    {
        return readCompoundMDArrayBlock(objectPath, type, blockDimensions, blockNumber, null);
    }

    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        final long[] offset = new long[blockDimensions.length];
        for (int i = 0; i < offset.length; ++i)
        {
            offset[i] = blockDimensions[i] * blockNumber[i];
        }
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset,
                inspectorOrNull);
    }

    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset)
            throws HDF5JavaException
    {
        return readCompoundMDArrayBlockWithOffset(objectPath, type, blockDimensions, offset, null);
    }

    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException
    {
        baseReader.checkOpen();
        return primReadCompoundArrayRankN(objectPath, type, blockDimensions, offset,
                inspectorOrNull);
    }

    private <T> MDArray<T> primReadCompoundArrayRankN(final String objectPath,
            final HDF5CompoundType<T> type, final int[] dimensionsOrNull,
            final long[] offsetOrNull, final IByteArrayInspector inspectorOrNull)
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
                            if (inspectorOrNull != null)
                            {
                                inspectorOrNull.inspect(byteArr);
                            }
                            return new MDArray<T>(type.getObjectByteifyer().arrayify(
                                    storageDataTypeId, byteArr, type.getCompoundType()),
                                    spaceParams.dimensions);
                        }
                    };
        return baseReader.runner.call(writeRunnable);
    }

}
