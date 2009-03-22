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
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_B64LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I8LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I16LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE;

import java.util.BitSet;
import java.util.Date;

import ncsa.hdf.hdf5lib.HDFNativeData;
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
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;

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
public final class HDF5Writer extends HDF5Reader implements HDF5SimpleWriter, IHDF5PrimitiveWriter
{
    /**
     * The constant to be used in the scaling writing functions.
     */
    public final static int AUTO_SCALING = 0;

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

    /**
     * Returns <code>true</code>, if the {@link HDF5BaseWriter} was <em>not</em> configured with
     * {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()}, that is if extendable data types
     * are used for new data sets.
     */
    public boolean isUseExtendableDataTypes()
    {
        return baseWriter.useExtentableDataTypes;
    }

    /**
     * Returns <code>true</code>, if the latest file format will be used and <code>false</code>, if
     * a file format with maximum compatibility will be used.
     */
    public boolean isUseLatestFileFormat()
    {
        return baseWriter.useLatestFileFormat;
    }

    // /////////////////////
    // File
    // /////////////////////

    /**
     * Flushes the cache to disk (without discarding it). Note that this may or may not trigger a
     * <code>fsync(2)</code>, depending on the {@link HDF5WriterConfigurator.SyncMode} used.
     */
    public void flush()
    {
        baseWriter.checkOpen();
        baseWriter.flush();
    }

    /**
     * Flushes the cache to disk (without discarding it) and synchronizes the file with the
     * underlying storage using a method like <code>fsync(2)</code>, regardless of what
     * {@link HDF5WriterConfigurator.SyncMode} has been set for this file.
     * <p>
     * This method blocks until <code>fsync(2)</code> has returned.
     */
    public void flushSyncBlocking()
    {
        baseWriter.checkOpen();
        baseWriter.flushSyncBlocking();
    }

    // /////////////////////
    // Objects & Links
    // /////////////////////

    /**
     * Creates a hard link.
     * 
     * @param currentPath The name of the data set (including path information) to create a link to.
     * @param newPath The name (including path information) of the link to create.
     */
    public void createHardLink(String currentPath, String newPath)
    {
        assert currentPath != null;
        assert newPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createHardLink(baseWriter.fileId, currentPath, newPath);
    }

    /**
     * Creates a soft link.
     * 
     * @param targetPath The name of the data set (including path information) to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     */
    public void createSoftLink(String targetPath, String linkPath)
    {
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        baseWriter.h5.createSoftLink(baseWriter.fileId, linkPath, targetPath);
    }

    /**
     * Creates or updates a soft link.
     * <p>
     * <em>Note: This method will never overwrite a data set, but only a symbolic link.</em>
     * 
     * @param targetPath The name of the data set (including path information) to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     */
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

    /**
     * Creates an external link, that is a link to a data set in another HDF5 file, the
     * <em>target</em> .
     * <p>
     * <em>Note: This method is only allowed when the {@link HDF5BaseWriter} was configured with 
     * {@link HDF5WriterConfigurator#useLatestFileFormat()}.</em>
     * 
     * @param targetFileName The name of the file where the data set resides that should be linked.
     * @param targetPath The name of the data set (including path information) in the
     *            <var>targetFileName</var> to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @throws IllegalStateException If the {@link HDF5BaseWriter} was not configured with
     *             {@link HDF5WriterConfigurator#useLatestFileFormat()}.
     */
    public void createExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException
    {
        assert targetFileName != null;
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        if (baseWriter.useLatestFileFormat == false)
        {
            throw new IllegalStateException("External links are not allowed with HDF5 1.6.x files.");
        }
        baseWriter.h5.createExternalLink(baseWriter.fileId, linkPath, targetFileName, targetPath);
    }

    /**
     * Creates or updates an external link, that is a link to a data set in another HDF5 file, the
     * <em>target</em> .
     * <p>
     * <em>Note: This method will never overwrite a data set, but only a symbolic link.</em>
     * <p>
     * <em>Note: This method is only allowed when the {@link HDF5BaseWriter} was configured with 
     * {@link HDF5WriterConfigurator#useLatestFileFormat()}.</em>
     * 
     * @param targetFileName The name of the file where the data set resides that should be linked.
     * @param targetPath The name of the data set (including path information) in the
     *            <var>targetFileName</var> to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @throws IllegalStateException If the {@link HDF5BaseWriter} was not configured with
     *             {@link HDF5WriterConfigurator#useLatestFileFormat()}.
     */
    public void createOrUpdateExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException
    {
        assert targetFileName != null;
        assert targetPath != null;
        assert linkPath != null;

        baseWriter.checkOpen();
        if (baseWriter.useLatestFileFormat == false)
        {
            throw new IllegalStateException("External links are not allowed with HDF5 1.6.x files.");
        }
        if (isSymbolicLink(linkPath))
        {
            delete(linkPath);
        }
        baseWriter.h5.createExternalLink(baseWriter.fileId, linkPath, targetFileName, targetPath);
    }

    /**
     * Removes an object from the file. If there is more than one link to the object, only the
     * specified link will be removed.
     */
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

    /**
     * Creates a group with path <var>objectPath</var> in the HDF5 file.
     * <p>
     * All intermediate groups will be created as well, if they do not already exist.
     * 
     * @param groupPath The path of the group to create.
     */
    public void createGroup(final String groupPath)
    {
        baseWriter.checkOpen();
        baseWriter.h5.createGroup(baseWriter.fileId, groupPath);
    }

    /**
     * Creates a group with path <var>objectPath</var> in the HDF5 file, giving the library a hint
     * about the size (<var>sizeHint</var>). If you have this information in advance, it will be
     * more efficient to tell it the library rather than to let the library figure out itself, but
     * the hint must not be misunderstood as a limit.
     * <p>
     * All intermediate groups will be created as well, if they do not already exist.
     * <p>
     * <i>Note: This method creates an "old-style group", that is the type of group of HDF5 1.6 and
     * earlier.</i>
     * 
     * @param groupPath The path of the group to create.
     * @param sizeHint The estimated size of all group entries (in bytes).
     */
    public void createGroup(final String groupPath, final int sizeHint)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            baseWriter.h5.createOldStyleGroup(baseWriter.fileId, groupPath,
                                    sizeHint, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    /**
     * Creates a group with path <var>objectPath</var> in the HDF5 file, giving the library hints
     * about when to switch between compact and dense. Setting appropriate values may improve
     * performance.
     * <p>
     * All intermediate groups will be created as well, if they do not already exist.
     * <p>
     * <i>Note: This method creates a "new-style group", that is the type of group of HDF5 1.8 and
     * above. Thus it will fail, if you didn't configure the file to be
     * {@link HDF5WriterConfigurator#useLatestFileFormat()}.</i>
     * 
     * @param groupPath The path of the group to create.
     * @param maxCompact When the group grows to more than this number of entries, the library will
     *            convert the group style from compact to dense.
     * @param minDense When the group shrinks below this number of entries, the library will convert
     *            the group style from dense to compact.
     */
    public void createGroup(final String groupPath, final int maxCompact, final int minDense)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            baseWriter.h5.createNewStyleGroup(baseWriter.fileId, groupPath,
                                    maxCompact, minDense, registry);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Deletes an attribute.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to delete the attribute from.
     * @param name The name of the attribute to delete.
     */
    public void deleteAttribute(final String objectPath, final String name)
    {
        baseWriter.checkOpen();
        final ICallableWithCleanUp<Object> addAttributeRunnable =
                new ICallableWithCleanUp<Object>()
                    {
                        public Object call(ICleanUpRegistry registry)
                        {
                            final int objectId =
                                    baseWriter.h5.openObject(baseWriter.fileId, objectPath,
                                            registry);
                            baseWriter.h5.deleteAttribute(objectId, name);
                            return null; // Nothing to return.
                        }
                    };
        baseWriter.runner.call(addAttributeRunnable);
    }

    /**
     * Adds an enum attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
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

    /**
     * Adds a <var>typeVariant</var> to <var>objectPath</var>.
     * 
     * @param objectPath The name of the object to add the type variant to.
     * @param typeVariant The type variant to add.
     */
    public void addTypeVariant(final String objectPath, final HDF5DataTypeVariant typeVariant)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()));
    }

    /**
     * Adds a <var>typeVariant</var> to <var>objectPath</var>.
     * 
     * @param objectId The id of the object to add the type variant to.
     * @param typeVariant The type variant to add.
     * @param registry The registry for clean up tasks.
     */
    private void addTypeVariant(final int objectId, final HDF5DataTypeVariant typeVariant,
            ICleanUpRegistry registry)
    {
        addAttribute(objectId, TYPE_VARIANT_ATTRIBUTE, baseWriter.typeVariantDataType
                .getStorageTypeId(), baseWriter.typeVariantDataType.getNativeTypeId(),
                baseWriter.typeVariantDataType.toStorageForm(typeVariant.ordinal()), registry);
    }

    /**
     * Adds a string attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addStringAttribute(final String objectPath, final String name, final String value)
    {
        addStringAttribute(objectPath, name, value, value.length());
    }

    /**
     * Adds a string attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @param maxLength The maximal length of the value.
     */
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

    /**
     * Adds a <code>boolean</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addBooleanAttribute(final String objectPath, final String name, final boolean value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, baseWriter.booleanDataTypeId, baseWriter.booleanDataTypeId,
                new byte[]
                    { (byte) (value ? 1 : 0) });
    }

    /**
     * Adds an <code>int</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addIntAttribute(final String objectPath, final String name, final int value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_STD_I32LE, H5T_NATIVE_INT32, HDFNativeData
                .intToByte(value));
    }

    /**
     * Adds a long attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addLongAttribute(final String objectPath, final String name, final long value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_STD_I64LE, H5T_NATIVE_INT64, HDFNativeData
                .longToByte(value));
    }

    /**
     * Adds a <code>float</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addFloatAttribute(final String objectPath, final String name, final float value)
    {
        baseWriter.checkOpen();
        addAttribute(objectPath, name, H5T_IEEE_F32LE, H5T_NATIVE_FLOAT, HDFNativeData
                .floatToByte(value));
    }

    /**
     * Adds a <code>double</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
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

    /**
     * Writes out a <code>boolean</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value of the data set.
     */
    public void writeBoolean(final String objectPath, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, HDFNativeData.byteToByte((byte) (value ? 1 : 0)));
    }

    /**
     * Writes out a bit field ((which can be considered the equivalent to a boolean array of rank
     * 1), provided as a Java {@link BitSet}. Uses a compact storage layout. Must only be used for
     * small data sets.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by this method cannot be read
     * back by {@link #readLongArray(String)} but will throw a
     * {@link ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
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

    /**
     * Writes out a bit field ((which can be considered the equivalent to a boolean array of rank
     * 1), provided as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by this method cannot be read
     * back by {@link #readLongArray(String)} but will throw a
     * {@link ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeBitField(final String objectPath, final BitSet data)
    {
        writeBitField(objectPath, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out a bit field ((which can be considered the equivalent to a boolean array of rank
     * 1), provided as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by this method cannot be read
     * back by {@link #readLongArray(String)} but will throw a
     * {@link ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out an opaque data type described by <var>tag</var> and defined by a <code>byte</code>
     * array (of rank 1). Uses a compact storage layout. Must only be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param tag The tag of the data set.
     * @param data The data to write. Must not be <code>null</code>.
     */
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

    /**
     * Writes out an opaque data type described by <var>tag</var> and defined by a <code>byte</code>
     * array (of rank 1).
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link #readAsByteArray(String)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param tag The tag of the data set.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data)
    {
        writeOpaqueByteArray(objectPath, tag, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out an opaque data type described by <var>tag</var> and defined by a <code>byte</code>
     * array (of rank 1).
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link #readAsByteArray(String)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param tag The tag of the data set.
     * @param data The data to write. Must not be <code>null</code>.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte vector to create.
     * @param blockSize The size of on block (for block-wise IO)
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     */
    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize)
    {
        return createOpaqueByteArray(objectPath, tag, size, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte vector to create.
     * @param blockSize The size of on block (for block-wise IO)
     * @param compression The compression parameters of the data set.
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     */
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

    /**
     * Writes out a block of an opaque data type represented by a <code>byte</code> array (of rank
     * 1). The data set needs to have been created by
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericCompression)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericCompression)} call that
     * was used to created the data set.
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link #readAsByteArrayBlock(String, int, long)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     */
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

    /**
     * Writes out a block of an opaque data type represented by a <code>byte</code> array (of rank
     * 1). The data set needs to have been created by
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericCompression)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericCompression)} call that
     * was used to created the data set.
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link #readAsByteArrayBlockWithOffset(String, int, long)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     */
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

    /**
     * Writes out a time stamp value. The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamp The timestamp to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     */
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

    /**
     * Creates a time stamp array (of rank 1). Uses a compact storage layout. Should only be used
     * for small data sets.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     */
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

    /**
     * Writes out a time stamp array (of rank 1). Uses a compact storage layout. Should only be used
     * for small data sets.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     */
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

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     */
    public void createTimeStampArray(final String objectPath, final long length, final int blockSize)
    {
        createTimeStampArray(objectPath, length, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     */
    public void writeTimeStampArray(final String objectPath, final long[] timeStamps)
    {
        writeTimeStampArray(objectPath, timeStamps, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericCompression)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createLongArray(String, long, int, HDF5IntCompression)} call that was used to create
     * the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     */
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

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericCompression)} beforehand.
     * <p>
     * Use this method instead of {@link #writeTimeStampArrayBlock(String, long[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createLongArray(String, long, int, HDF5IntCompression)} call that was used to create
     * the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     */
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

    /**
     * Writes out a time stamp value provided as a {@link Date}. The data set will be tagged as type
     * variant {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param date The date to write.
     * @see #writeTimeStamp(String, long)
     */
    public void writeDate(final String objectPath, final Date date)
    {
        writeTimeStamp(objectPath, date.getTime());
    }

    /**
     * Writes out a {@link Date} array (of rank 1). Uses a compact storage layout. Should only be
     * used for small data sets.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @see #writeTimeStampArrayCompact(String, long[])
     */
    public void writeDateArrayCompact(final String objectPath, final Date[] dates)
    {
        writeTimeStampArrayCompact(objectPath, datesToTimeStamps(dates));
    }

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @see #writeTimeStampArray(String, long[])
     */
    public void writeDateArray(final String objectPath, final Date[] dates)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates));
    }

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @param compression The compression parameters of the data set.
     * @see #writeTimeStampArray(String, long[], HDF5GenericCompression)
     */
    public void writeDateArray(final String objectPath, final Date[] dates,
            final HDF5GenericCompression compression)
    {
        writeTimeStampArray(objectPath, datesToTimeStamps(dates), compression);
    }

    /**
     * Converts an array of {@link Date}s into an array of time stamps.
     */
    public static long[] datesToTimeStamps(Date[] dates)
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

    /**
     * Writes out a time duration value in seconds. The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in seconds.
     */
    public void writeTimeDuration(final String objectPath, final long timeDuration)
    {
        writeTimeDuration(objectPath, timeDuration, HDF5TimeUnit.SECONDS);
    }

    /**
     * Writes out a time duration value. The data set will be tagged as the according type variant.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     */
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

    /**
     * Creates a time duration array (of rank 1). Uses a compact storage layout. Should only be used
     * for small data sets. The data set will be tagged as the according type variant.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     * @param timeUnit The unit of the time duration.
     */
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

    /**
     * Writes out a time duration array (of rank 1). Uses a compact storage layout. Should only be
     * used for small data sets. The data set will be tagged as the according type variant.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     */
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

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     */
    public void createTimeDurationArray(final String objectPath, final long length,
            final int blockSize, final HDF5TimeUnit timeUnit)
    {
        createTimeDurationArray(objectPath, length, blockSize, timeUnit,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out a time duration array in seconds (of rank 1). The data set will be tagged as type
     * variant {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in seconds.
     */
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations)
    {
        writeTimeDurationArray(objectPath, timeDurations, HDF5TimeUnit.SECONDS,
                HDF5IntCompression.INT_NO_COMPRESSION);
    }

    /**
     * Writes out a time duration array (of rank 1). The data set will be tagged as the according
     * type variant.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     */
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit)
    {
        writeTimeDurationArray(objectPath, timeDurations, timeUnit,
                HDF5IntCompression.INT_NO_COMPRESSION);
    }

    /**
     * Writes out a time duration array (of rank 1). The data set will be tagged as the according
     * type variant.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericCompression)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericCompression)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     */
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

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericCompression)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericCompression)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     */
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

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of the <var>data</var>.
     */
    public void writeString(final String objectPath, final String data, final int maxLength)
    {
        writeString(objectPath, data, maxLength, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out a <code>String</code> with a fixed maximal length (which is the length of the
     * string <var>data</var>).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeString(final String objectPath, final String data)
    {
        writeString(objectPath, data, data.length(), HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param compression The compression parameters of the data set.
     */
    public void writeString(final String objectPath, final String data,
            final HDF5GenericCompression compression)
    {
        writeString(objectPath, data, data.length(), compression);
    }

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of the <var>data</var>.
     * @param compression The compression parameters of the data set.
     */
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
                                baseWriter.h5
                                        .createDataSet(baseWriter.fileId,
                                                HDF5Utils.SCALAR_DIMENSIONS, chunkSizeOrNull,
                                                stringDataTypeId, compression, objectPath, layout,
                                                registry);
                    }
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            (data + '\0').getBytes());
                    return null; // Nothing to return.
                }

            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>String</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param compression The compression parameters of the data set.
     */
    public void writeStringArray(final String objectPath, final String[] data,
            final HDF5GenericCompression compression)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data), compression);
    }

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is defined by the longest string in <var>data</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeStringArray(final String objectPath, final String[] data)
    {
        assert objectPath != null;
        assert data != null;

        writeStringArray(objectPath, data, getMaxLength(data),
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     */
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

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     * @param compression The compression parameters of the data set.
     */
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
                                        layout, registry);
                    }
                    H5Dwrite(dataSetId, stringDataTypeId, H5S_ALL, H5S_ALL, H5P_DEFAULT, data,
                            maxLength);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a <code>String</code> with variable maximal length.
     * <p>
     * The advantage of this method over {@link #writeString(String, String)} is that when writing a
     * new string later it can have a different (also greater) length. The disadvantage is that it
     * it is more time consuming to read and write this kind of string and that it can't be
     * compressed.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
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

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file, if necessary creating it. If
     * it does already exist, the values of the type will be checked against <var>values</var>.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @throws HDF5JavaException If the data type exists and is not compatible with the
     *             <var>values</var> provided.
     */
    @Override
    public HDF5EnumerationType getEnumType(final String name, final String[] values)
            throws HDF5JavaException
    {
        return getEnumType(name, values, true);
    }

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file, if necessary creating it.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     * @param values The values of the enumeration.
     * @param check If <code>true</code> and if the data type already exists, check whether it is
     *            compatible with the <var>values</var> provided.
     * @throws HDF5JavaException If <code>check = true</code>, the data type exists and is not
     *             compatible with the <var>values</var> provided.
     */
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

    /**
     * Writes out an enum value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value of the data set.
     * @throws HDF5JavaException If the enum type of <var>value</var> is not a type of this file.
     */
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

    /**
     * Writes out an array of enum values. Uses a compact storage layout. Must only be used for
     * small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @throws HDF5JavaException If the enum type of <var>value</var> is not a type of this file.
     */
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

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @throws HDF5JavaException If the enum type of <var>value</var> is not a type of this file.
     */
    public void writeEnumArray(final String objectPath, final HDF5EnumerationValueArray data)
            throws HDF5JavaException
    {
        writeEnumArray(objectPath, data, HDF5IntCompression.INT_NO_COMPRESSION);
    }

    /**
     * Writes out an array of enum values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param compression The compression parameters of the data set. Note that for scaling
     *            compression the compression factor is ignored. Instead, the scaling factor is
     *            computed from the number of entries in the enumeration.
     * @throws HDF5JavaException If the enum type of <var>value</var> is not a type of this file.
     */
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

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, if necessary creating it.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
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

    /**
     * Returns the compound type <var>name></var> for this HDF5 file, if necessary creating it.
     * 
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
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

    /**
     * Writes out an array (of rank 1) of compound values. Uses a compact storage layout. Must only
     * be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     */
    public <T> void writeCompound(final String objectPath, final HDF5CompoundType<T> type,
            final T data)
    {
        baseWriter.checkOpen();
        type.check(baseWriter.fileId);
        baseWriter.writeScalar(objectPath, type.getStorageTypeId(), type.getNativeTypeId(), type
                .getObjectByteifyer().byteify(type.getStorageTypeId(), data));
    }

    /**
     * Writes out an array (of rank 1) of compound values. Uses a compact storage layout. Must only
     * be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     */
    public <T> void writeCompoundArrayCompact(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data)
    {
        writeCompoundArrayCompact(objectPath, type, data,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out an array (of rank 1) of compound values. Uses a compact storage layout. Must only
     * be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     * @param compression The compression parameters of the data set.
     */
    public <T> void writeCompoundArrayCompact(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final HDF5GenericCompression compression)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     */
    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data)
    {
        writeCompoundArray(objectPath, type, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     * @param compression The compression parameters of the data set.
     */
    public <T> void writeCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final T[] data, final HDF5GenericCompression compression)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a block <var>blockNumber</var> of an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     * @param blockNumber The number of the block to write.
     */
    public <T> void writeCompoundArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long blockNumber)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a block of an array (of rank 1) of compound values with given <var>offset</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The value of the data set.
     * @param offset The offset of the block in the data set.
     */
    public <T> void writeCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final T[] data, final long offset)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Creates an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param size The size of the compound array to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     */
    public <T> void createCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final long size, final int blockSize)
    {
        createCompoundArray(objectPath, type, size, blockSize,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Creates an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param size The size of the compound array to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param compression The compression parameters of the data set.
     */
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

    /**
     * Writes out an array (of rank N) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The data to write.
     */
    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data)
    {
        writeCompoundMDArray(objectPath, type, data, HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Writes out an array (of rank N) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The data to write.
     * @param compression The compression parameters of the data set.
     */
    public <T> void writeCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final MDArray<T> data, final HDF5GenericCompression compression)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a block of an array (of rank N) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The data to write.
     * @param blockDimensions The extent of the block to write on each axis.
     */
    public <T> void writeCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] blockDimensions)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a block of an array (of rank N) of compound values give a given <var>offset</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The data to write.
     * @param offset The offset of the block to write on each axis.
     */
    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final long[] offset)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Writes out a block of an array (of rank N) of compound values give a given <var>offset</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param data The data to write.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     */
    public <T> void writeCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final MDArray<T> data, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset)
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
                    H5Dwrite(dataSetId, type.getNativeTypeId(), memorySpaceId, dataSpaceId,
                            H5P_DEFAULT, byteArray);
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

    /**
     * Creates an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param dimensions The extent of the compound array along each of the axis.
     * @param blockDimensions The extent of one block along each of the axis. (for block-wise IO).
     *            Ignored if no extendable data sets are used (see
     *            {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()}) and
     *            <code>deflate == false</code>.
     */
    public <T> void createCompoundMDArray(final String objectPath, final HDF5CompoundType<T> type,
            final long[] dimensions, final int[] blockDimensions)
    {
        createCompoundMDArray(objectPath, type, dimensions, blockDimensions,
                HDF5GenericCompression.GENERIC_NO_COMPRESSION);
    }

    /**
     * Creates an array (of rank 1) of compound values.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param dimensions The extent of the compound array along each of the axis.
     * @param blockDimensions The extent of one block along each of the axis. (for block-wise IO).
     *            Ignored if no extendable data sets are used (see
     *            {@link HDF5WriterConfigurator#dontUseExtendableDataTypes()}) and
     *            <code>deflate == false</code>.
     * @param compression The compression parameters of the data set.
     */
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
