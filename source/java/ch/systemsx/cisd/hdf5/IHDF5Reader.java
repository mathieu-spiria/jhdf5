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

import java.io.File;
import java.util.BitSet;
import java.util.Date;
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;

/**
 * An interface for reading HDF5 files (HDF5 1.8.x and older).
 * <p>
 * The interface focuses on ease of use instead of completeness. As a consequence not all features
 * of a valid HDF5 files can be read using this class, but only a subset. (All information written
 * by {@link IHDF5Writer} can be read by this class.)
 * <p>
 * Usage:
 * 
 * <pre>
 * IHDF5Reader reader = HDF5FactoryProvider.get().openForReading(&quot;test.h5&quot;);
 * float[] f = reader.readFloatArray(&quot;/some/path/dataset&quot;);
 * String s = reader.getAttributeString(&quot;/some/path/dataset&quot;, &quot;some key&quot;);
 * reader.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
public interface IHDF5Reader extends IHDF5SimpleReader, IHDF5PrimitiveReader
{

    /**
     * An interface for inspecting the byte array of compounds and compound arrays just after they
     * are read from or before they are written to the HDF5 file.
     */
    public interface IByteArrayInspector
    {
        /**
         * Called with the byte array. The method can change the <var>byteArray</var> but does so on
         * its own risk!.
         */
        void inspect(byte[] byteArray);
    }

    // /////////////////////
    // Configuration
    // /////////////////////

    /**
     * Returns <code>true</code>, if the latest file format will be used and <code>false</code>, if
     * a file format with maximum compatibility will be used.
     */
    public boolean isPerformNumericConversions();

    /**
     * Returns the HDF5 file that this class is reading.
     */
    public File getFile();

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    public void close();

    // /////////////////////
    // Objects & Links
    // /////////////////////

    /**
     * Returns the link information for the given <var>objectPath</var>. If <var>objectPath</var>
     * does not exist, the link information will have a type {@link HDF5ObjectType#NONEXISTENT}.
     */
    public HDF5LinkInformation getLinkInformation(final String objectPath);

    /**
     * Returns the object information for the given <var>objectPath</var>. If <var>objectPath</var>
     * is a symbolic link, this method will return the type of the object that this link points to
     * rather than the type of the link. If <var>objectPath</var> does not exist, the object
     * information will have a type {@link HDF5ObjectType#NONEXISTENT} and the other fields will not
     * be set.
     */
    public HDF5ObjectInformation getObjectInformation(final String objectPath);

    /**
     * Returns the type of the given <var>objectPath<var>. If <var>followLink</var> is
     * <code>false</code> and <var>objectPath<var> is a symbolic link, this method will return the
     * type of the link rather than the type of the object that the link points to.
     */
    public HDF5ObjectType getObjectType(final String objectPath, boolean followLink);

    /**
     * Returns the type of the given <var>objectPath</var>. If <var>objectPath</var> is a symbolic
     * link, this method will return the type of the object that this link points to rather than the
     * type of the link, that is, it will follow symbolic links.
     */
    public HDF5ObjectType getObjectType(final String objectPath);

    /**
     * Returns <code>true</code>, if <var>objectPath</var> exists and <code>false</code> otherwise.
     * if <var>followLink</var> is <code>false</code> and <var>objectPath</var> is a symbolic link,
     * this method will return <code>true</code> regardless of whether the link target exists or
     * not.
     */
    public boolean exists(final String objectPath, boolean followLink);

    /**
     * Returns <code>true</code>, if <var>objectPath</var> exists and <code>false</code> otherwise.
     * If <var>objectPath</var> is a symbolic link, the method will return <code>true</code> if the
     * link target exists, that is, this method will follow symbolic links.
     */
    public boolean exists(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a group and
     * <code>false</code> otherwise. Note that if <var>followLink</var> is <code>false</code> this
     * method will return <code>false</code> if <var>objectPath</var> is a symbolic link that points
     * to a group.
     */
    public boolean isGroup(final String objectPath, boolean followLink);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a group and
     * <code>false</code> otherwise. Note that if <var>objectPath</var> is a symbolic link, this
     * method will return <code>true</code> if the link target of the symbolic link is a group, that
     * is, this method will follow symbolic links.
     */
    public boolean isGroup(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data set and
     * <code>false</code> otherwise. Note that if <var>followLink</var> is <code>false</code> this
     * method will return <code>false</code> if <var>objectPath</var> is a symbolic link that points
     * to a data set.
     */
    public boolean isDataSet(final String objectPath, boolean followLink);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data set and
     * <code>false</code> otherwise. Note that if <var>objectPath</var> is a symbolic link, this
     * method will return <code>true</code> if the link target of the symbolic link is a data set,
     * that is, this method will follow symbolic links.
     */
    public boolean isDataSet(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data type and
     * <code>false</code> otherwise. Note that if <var>followLink</var> is <code>false</code> this
     * method will return <code>false</code> if <var>objectPath</var> is a symbolic link that points
     * to a data type.
     */
    public boolean isDataType(final String objectPath, boolean followLink);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a data type and
     * <code>false</code> otherwise. Note that if <var>objectPath</var> is a symbolic link, this
     * method will return <code>true</code> if the link target of the symbolic link is a data type,
     * that is, this method will follow symbolic links.
     */
    public boolean isDataType(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents a soft link and
     * <code>false</code> otherwise.
     */
    public boolean isSoftLink(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents an external link
     * and <code>false</code> otherwise.
     */
    public boolean isExternalLink(final String objectPath);

    /**
     * Returns <code>true</code> if the <var>objectPath</var> exists and represents either a soft
     * link or an external link and <code>false</code> otherwise.
     */
    public boolean isSymbolicLink(final String objectPath);

    /**
     * Returns the target of the symbolic link that <var>objectPath</var> points to, or
     * <code>null</code>, if <var>objectPath</var> is not a symbolic link.
     */
    public String tryGetSymbolicLinkTarget(final String objectPath);

    /**
     * Returns the path of the data type of the data set <var>objectPath</var>, or <code>null</code>
     * , if this data set is not of a named data type.
     */
    public String tryGetDataTypePath(final String objectPath);

    /**
     * Returns the path of the data <var>type</var>, or <code>null</code>, if <var>type</var> is not
     * a named data type.
     */
    public String tryGetDataTypePath(HDF5DataType type);

    /**
     * Returns the names of the attributes of the given <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the object (data set or group) to
     *            return the attributes for.
     */
    public List<String> getAttributeNames(final String objectPath);

    /**
     * Returns the names of all attributes of the given <var>objectPath</var>.
     * <p>
     * This may include attributes that are used internally by the library and are not supposed to
     * be changed by application programmers.
     * 
     * @param objectPath The name (including path information) of the object (data set or group) to
     *            return the attributes for.
     */
    public List<String> getAllAttributeNames(final String objectPath);

    /**
     * Returns the information about a data set as a {@link HDF5DataTypeInformation} object.
     * 
     * @param dataSetPath The name (including path information) of the data set to return
     *            information about.
     * @param attributeName The name of the attribute to get information about.
     */
    public HDF5DataTypeInformation getAttributeInformation(final String dataSetPath,
            final String attributeName);

    /**
     * Returns the information about a data set as a {@link HDF5DataTypeInformation} object. It is a
     * failure condition if the <var>dataSetPath</var> does not exist or does not identify a data
     * set.
     * 
     * @param dataSetPath The name (including path information) of the data set to return
     *            information about.
     */
    public HDF5DataSetInformation getDataSetInformation(final String dataSetPath);

    /**
     * Returns the total size (in bytes) of <var>objectPath</var>, if it is a data set. If it exists
     * but is not data set, it return 0 and if it does not exist, -1 is returned.
     */
    public long getSize(final String objectPath);

    /**
     * Returns the total number of elements of <var>objectPath</var>, if it is a data set. If it
     * exists but is not data set, it return 0 and if it does not exist, -1 is returned.
     */
    public long getNumberOfElements(final String objectPath);

    // /////////////////////
    // Group
    // /////////////////////

    /**
     * Returns the members of <var>groupPath</var>. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getGroupMembers(final String groupPath);

    /**
     * Returns all members of <var>groupPath</var>, including internal groups that may be used by
     * the library to do house-keeping. The order is <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the members for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getAllGroupMembers(final String groupPath);

    /**
     * Returns the paths of the members of <var>groupPath</var> (including the parent). The order is
     * <i>not</i> well defined.
     * 
     * @param groupPath The path of the group to get the member paths for.
     * @throws IllegalArgumentException If <var>groupPath</var> is not a group.
     */
    public List<String> getGroupMemberPaths(final String groupPath);

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
            boolean readLinkTargets);

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
            boolean readLinkTargets);

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
    public String tryGetOpaqueTag(final String objectPath);

    /**
     * Returns the opaque data type or <code>null</code>, if <var>objectPath</var> is not of such a
     * data type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The opaque data type, or <code>null</code>.
     */
    public HDF5OpaqueType tryGetOpaqueType(final String objectPath);

    /**
     * Returns the enumeration type <var>name</var> for this HDF5 file. Use this method only when
     * you know that the type exists.
     * 
     * @param name The name of the enumeration in the HDF5 file.
     */
    public HDF5EnumerationType getEnumType(final String name);

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
            throws HDF5JavaException;

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
            final boolean check) throws HDF5JavaException;

    /**
     * Returns the enumeration type for the data set <var>dataSetPath</var>.
     * 
     * @param dataSetPath The name of data set to get the enumeration type for.
     */
    public HDF5EnumerationType getEnumTypeForObject(final String dataSetPath);

    /**
     * Returns the data type variant of <var>objectPath</var>, or <code>null</code>, if no type
     * variant is defined for this <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data type variant or <code>null</code>.
     */
    public HDF5DataTypeVariant tryGetTypeVariant(final String objectPath);

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
    public boolean hasAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>String</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public String getStringAttribute(final String objectPath, final String attributeName);

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
            throws HDF5JavaException;

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
            throws HDF5JavaException;

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
            throws HDF5JavaException;

    /**
     * Reads an <code>int</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public int getIntAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>long</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public long getLongAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>float</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public float getFloatAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>double</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     */
    public double getDoubleAttribute(final String objectPath, final String attributeName);

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
    public byte[] readAsByteArray(final String objectPath);

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
            final long blockNumber) throws HDF5JavaException;

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
            final long offset) throws HDF5JavaException;

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
            throws HDF5JavaException;

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
    public boolean readBoolean(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a bit field (which can be considered the equivalent to a boolean array of rank 1) from
     * the data set <var>objectPath</var> and returns it as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by
     * {@link IHDF5Writer#writeLongArray(String, long[])} cannot be read back by this method but
     * will throw a {@link HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The {@link BitSet} read from the data set.
     * @throws HDF5DatatypeInterfaceException If the <var>objectPath</var> is not of bit field type.
     */
    public BitSet readBitField(final String objectPath) throws HDF5DatatypeInterfaceException;

    //
    // Time stamp
    //

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time stamp and
     * <code>false</code> otherwise.
     */
    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException;

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
    public long readTimeStamp(final String objectPath) throws HDF5JavaException;

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
    public long[] readTimeStampArray(final String objectPath) throws HDF5JavaException;

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
            final long blockNumber);

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
            final long offset);

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<long[]>> getTimeStampArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

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
    public Date readDate(final String objectPath) throws HDF5JavaException;

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
    public Date[] readDateArray(final String objectPath) throws HDF5JavaException;

    //
    // Duration
    //

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     */
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     */
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException;

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
    public long readTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time duration value from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged as one of the
     * type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link IHDF5Writer#addTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
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
            throws HDF5JavaException;

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
    public long[] readTimeDurationArray(final String objectPath) throws HDF5JavaException;

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
            throws HDF5JavaException;

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
            final long blockNumber, final HDF5TimeUnit timeUnit);

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
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit);

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @param timeUnit The time unit that the duration should be converted to.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(
            final String dataSetPath, final HDF5TimeUnit timeUnit) throws HDF5JavaException;

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
    public String readString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a <code>String</code> array (of rank 1) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArray(final String objectPath) throws HDF5JavaException;

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
    public String readEnumAsString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValue readEnum(final String objectPath) throws HDF5JavaException;

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
            throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param enumType The enumeration type of this array.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath,
            final HDF5EnumerationType enumType) throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> value from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not of <var>enumType</var>.
     */
    public HDF5EnumerationValueArray readEnumArray(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads an <code>Enum</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set as an array of Strings.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public String[] readEnumArrayAsString(final String objectPath) throws HDF5JavaException;

    //
    // Compound
    //

    /**
     * Returns the member information for the committed compound data type <var>compoundClass</var>
     * (using its "simple name"). The returned array will contain the members in alphabetical order.
     * It is a failure condition if this compound data type does not exist.
     */
    public <T> HDF5CompoundMemberInformation[] getCompoundMemberInformation(
            final Class<T> compoundClass);

    /**
     * Returns the member information for the committed compound data type <var>dataTypeName</var>.
     * The returned array will contain the members in alphabetical order. It is a failure condition
     * if this compound data type does not exist.
     */
    public HDF5CompoundMemberInformation[] getCompoundMemberInformation(final String dataTypeName);

    /**
     * Returns the compound member information for the data set <var>dataSetPath</var>. The returned
     * array will contain the members in alphabetical order. It is a failure condition if this data
     * set does not exist or is not of compound type.
     * 
     * @throws HDF5JavaException If the data set is not of type compound.
     */
    public HDF5CompoundMemberInformation[] getCompoundDataSetInformation(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param name The name of the compound in the HDF5 file.
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getCompoundType(final String name, final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members);

    /**
     * Returns the compound type <var>name></var> for this HDF5 file.
     * 
     * @param compoundType The Java type that corresponds to this HDF5 type.
     * @param members The mapping from the Java compound type to the HDF5 type.
     */
    public <T> HDF5CompoundType<T> getCompoundType(final Class<T> compoundType,
            final HDF5CompoundMemberMapping... members);

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound type.
     */
    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException;

    /**
     * Reads a compound from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into a Java object.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a compound type.
     */
    public <T> T readCompound(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type)
            throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArray(final String objectPath, final HDF5CompoundType<T> type,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

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
            final int blockSize, final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArrayBlock(final String objectPath, final HDF5CompoundType<T> type,
            final int blockSize, final long blockNumber, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

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
            throws HDF5JavaException;

    /**
     * Reads a compound array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockSize The block size (this will be the length of the <code>float[]</code> returned
     *            if the data set is long enough).
     * @param offset The offset of the block to read (starting with 0).
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> T[] readCompoundArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int blockSize, final long offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of compounds to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     */
    public <T> Iterable<HDF5DataBlock<T[]>> getCompoundArrayNaturalBlocks(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type) throws HDF5JavaException;

    /**
     * Reads a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArray(final String objectPath,
            final HDF5CompoundType<T> type, final IByteArrayInspector inspectorOrNull)
            throws HDF5JavaException;

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
            throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param blockNumber The number of the block to write along each axis.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArrayBlock(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] blockNumber,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException;

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
            throws HDF5JavaException;

    /**
     * Reads a block from a compound array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param type The type definition of this compound type.
     * @param blockDimensions The extent of the block to write along each axis.
     * @param offset The offset of the block to write in the data set along each axis.
     * @param inspectorOrNull The inspector to be called before the byte array read from the HDF5
     *            file is translated back into Java objects.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not an enum type.
     */
    public <T> MDArray<T> readCompoundMDArrayBlockWithOffset(final String objectPath,
            final HDF5CompoundType<T> type, final int[] blockDimensions, final long[] offset,
            final IByteArrayInspector inspectorOrNull) throws HDF5JavaException;
}
