/*
 * Copyright 2013 ETH Zuerich, CISD
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

import java.io.Flushable;
import java.util.BitSet;
import java.util.Date;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5SymbolTableException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

/**
 * The legacy interface for writing HDF5 files. Do not use in any new code as it will be removed in
 * a future version of JHDF5.
 * 
 * @author Bernd Rinn
 */
@Deprecated
public interface IHDF5LegacyWriter extends IHDF5EnumBasicWriter, IHDF5CompoundBasicWriter
{
    // *********************
    // File level
    // *********************

    // /////////////////////
    // Configuration
    // /////////////////////

    /**
     * Returns <code>true</code>, if the {@link IHDF5WriterConfigurator} was <em>not</em> configured
     * with {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}, that is if extendable data
     * types are used for new data sets.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public boolean isUseExtendableDataTypes();

    /**
     * Returns the {@link FileFormat} compatibility setting for this writer.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public FileFormat getFileFormat();

    // /////////////////////
    // Flushing and Syncing
    // /////////////////////

    /**
     * Flushes the cache to disk (without discarding it). Note that this may or may not trigger a
     * <code>fsync(2)</code>, depending on the {@link IHDF5WriterConfigurator.SyncMode} used.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public void flush();

    /**
     * Flushes the cache to disk (without discarding it) and synchronizes the file with the
     * underlying storage using a method like <code>fsync(2)</code>, regardless of what
     * {@link IHDF5WriterConfigurator.SyncMode} has been set for this file.
     * <p>
     * This method blocks until <code>fsync(2)</code> has returned.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public void flushSyncBlocking();

    /**
     * Adds a {@link Flushable} to the set of flushables. This set is flushed when {@link #flush()}
     * or {@link #flushSyncBlocking()} are called and before the writer is closed.
     * <p>
     * This function is supposed to be used for in-memory caching structures that need to make it
     * into the HDF5 file.
     * <p>
     * If the <var>flushable</var> implements
     * {@link ch.systemsx.cisd.base.exceptions.IErrorStrategy}, in case of an exception in
     * {@link Flushable#flush()}, the method
     * {@link ch.systemsx.cisd.base.exceptions.IErrorStrategy#dealWithError(Throwable)} will be
     * called to decide how do deal with the exception.
     * 
     * @param flushable The {@link Flushable} to add. Needs to fulfill the {@link Object#hashCode()}
     *            contract.
     * @return <code>true</code> if the set of flushables did not already contain the specified
     *         element.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public boolean addFlushable(Flushable flushable);

    /**
     * Removes a {@link Flushable} from the set of flushables.
     * 
     * @param flushable The {@link Flushable} to remove. Needs to fulfill the
     *            {@link Object#hashCode()} contract.
     * @return <code>true</code> if the set of flushables contained the specified element.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#file()} instead.
     */
    @Deprecated
    public boolean removeFlushable(Flushable flushable);

    // ***********************
    // Objects, Links, Groups
    // ***********************

    /**
     * Creates a hard link.
     * 
     * @param currentPath The name of the data set (including path information) to create a link to.
     * @param newPath The name (including path information) of the link to create.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createHardLink(String currentPath, String newPath);

    /**
     * Creates a soft link.
     * 
     * @param targetPath The name of the data set (including path information) to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createSoftLink(String targetPath, String linkPath);

    /**
     * Creates or updates a soft link.
     * <p>
     * <em>Note: This method will never overwrite a data set, but only a symbolic link.</em>
     * 
     * @param targetPath The name of the data set (including path information) to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createOrUpdateSoftLink(String targetPath, String linkPath);

    /**
     * Creates an external link, that is a link to a data set in another HDF5 file, the
     * <em>target</em> .
     * <p>
     * <em>Note: This method is only allowed when the {@link IHDF5WriterConfigurator} was not 
     * configured to enforce strict HDF5 1.6 compatibility.</em>
     * 
     * @param targetFileName The name of the file where the data set resides that should be linked.
     * @param targetPath The name of the data set (including path information) in the
     *            <var>targetFileName</var> to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @throws IllegalStateException If the {@link IHDF5WriterConfigurator} was configured to
     *             enforce strict HDF5 1.6 compatibility.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException;

    /**
     * Creates or updates an external link, that is a link to a data set in another HDF5 file, the
     * <em>target</em> .
     * <p>
     * <em>Note: This method will never overwrite a data set, but only a symbolic link.</em>
     * <p>
     * <em>Note: This method is only allowed when the {@link IHDF5WriterConfigurator} was not 
     * configured to enforce strict HDF5 1.6 compatibility.</em>
     * 
     * @param targetFileName The name of the file where the data set resides that should be linked.
     * @param targetPath The name of the data set (including path information) in the
     *            <var>targetFileName</var> to create a link to.
     * @param linkPath The name (including path information) of the link to create.
     * @throws IllegalStateException If the {@link IHDF5WriterConfigurator} was configured to
     *             enforce strict HDF5 1.6 compatibility.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createOrUpdateExternalLink(String targetFileName, String targetPath, String linkPath)
            throws IllegalStateException;

    /**
     * Moves or renames a link in the file atomically.
     * 
     * @throws HDF5SymbolTableException If <var>oldLinkPath</var> does not exist or if
     *             <var>newLinkPath</var> already exists.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void move(String oldLinkPath, String newLinkPath) throws HDF5SymbolTableException;

    // /////////////////////
    // Group
    // /////////////////////

    /**
     * Creates a group with path <var>objectPath</var> in the HDF5 file.
     * <p>
     * All intermediate groups will be created as well, if they do not already exist.
     * 
     * @param groupPath The path of the group to create.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createGroup(final String groupPath);

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
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createGroup(final String groupPath, final int sizeHint);

    /**
     * Creates a group with path <var>objectPath</var> in the HDF5 file, giving the library hints
     * about when to switch between compact and dense. Setting appropriate values may improve
     * performance.
     * <p>
     * All intermediate groups will be created as well, if they do not already exist.
     * <p>
     * <i>Note: This method creates a "new-style group", that is the type of group of HDF5 1.8 and
     * above. Thus it will fail, if the writer is configured to enforce HDF5 1.6 compatibility.</i>
     * 
     * @param groupPath The path of the group to create.
     * @param maxCompact When the group grows to more than this number of entries, the library will
     *            convert the group style from compact to dense.
     * @param minDense When the group shrinks below this number of entries, the library will convert
     *            the group style from dense to compact.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void createGroup(final String groupPath, final int maxCompact, final int minDense);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Sets the data set size of a one-dimensional data set to <var>newSize</var>. Note that this
     * method can only be applied to extendable data sets.
     * 
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not extendable.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void setDataSetSize(final String objectPath, final long newSize);

    /**
     * Sets the data set size of a multi-dimensional data set to <var>newDimensions</var>. Note that
     * this method can only be applied to extendable data sets.
     * 
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not extendable.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void setDataSetDimensions(final String objectPath, final long[] newDimensions);

    // /////////////////////
    // Types
    // /////////////////////

    /**
     * Sets a <var>typeVariant</var> of object <var>objectPath</var>.
     * 
     * @param objectPath The name of the object to add the type variant to.
     * @param typeVariant The type variant to add.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void setTypeVariant(final String objectPath, final HDF5DataTypeVariant typeVariant);

    /**
     * Sets a <var>typeVariant</var> of attribute <var>attributeName</var> of object
     * <var>objectPath</var>.
     * 
     * @param objectPath The name of the object.
     * @param attributeName The name of attribute to add the type variant to.
     * @param typeVariant The type variant to add.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void setTypeVariant(final String objectPath, final String attributeName,
            final HDF5DataTypeVariant typeVariant);

    /**
     * Deletes the <var>typeVariant</var> from <var>objectPath</var>.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to delete the type variant from.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void deleteTypeVariant(final String objectPath);

    /**
     * Deletes the <var>typeVariant</var> from <var>attributeName</var> of <var>objectPath</var>.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object.
     * @param attributeName The name of the attribute to delete the type variant from.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void deleteTypeVariant(final String objectPath, final String attributeName);

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
     * @deprecated Use the corresponding method in {@link IHDF5Writer#object()} instead.
     */
    @Deprecated
    public void deleteAttribute(final String objectPath, final String name);

    // *********************
    // Opaque
    // *********************

    /**
     * Writes out an opaque data type described by <var>tag</var> and defined by a <code>byte</code>
     * array (of rank 1).
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link IHDF5OpaqueReader#readArray(String)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param tag The tag of the data set.
     * @param data The data to write. Must not be <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data);

    /**
     * Writes out an opaque data type described by <var>tag</var> and defined by a <code>byte</code>
     * array (of rank 1).
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link IHDF5OpaqueReader#readArray(String)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param tag The tag of the data set.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public void writeOpaqueByteArray(final String objectPath, final String tag, final byte[] data,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create.
     * @param blockSize The size of on block (for block-wise IO)
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize);

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final int size);

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create.
     * @param blockSize The size of on block (for block-wise IO)
     * @param features The storage features of the data set.
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final long size, final int blockSize, final HDF5GenericStorageFeatures features);

    /**
     * Creates an opaque data set that will be represented as a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param features The storage features of the data set.
     * @return The {@link HDF5OpaqueType} that can be used in methods
     *         {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} and
     *         {@link #writeOpaqueByteArrayBlockWithOffset(String, HDF5OpaqueType, byte[], int, long)}
     *         to represent this opaque type.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public HDF5OpaqueType createOpaqueByteArray(final String objectPath, final String tag,
            final int size, final HDF5GenericStorageFeatures features);

    /**
     * Writes out a block of an opaque data type represented by a <code>byte</code> array (of rank
     * 1). The data set needs to have been created by
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericStorageFeatures)} call
     * that was used to created the data set.
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link IHDF5OpaqueReader#readArrayBlock(String, int, long)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public void writeOpaqueByteArrayBlock(final String objectPath, final HDF5OpaqueType dataType,
            final byte[] data, final long blockNumber);

    /**
     * Writes out a block of an opaque data type represented by a <code>byte</code> array (of rank
     * 1). The data set needs to have been created by
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeOpaqueByteArrayBlock(String, HDF5OpaqueType, byte[], long)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createOpaqueByteArray(String, String, long, int, HDF5GenericStorageFeatures)} call
     * that was used to created the data set.
     * <p>
     * Note that there is no dedicated method for reading opaque types. Use the method
     * {@link IHDF5OpaqueReader#readArrayBlockWithOffset(String, int, long)} instead.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#opaque()} instead.
     */
    @Deprecated
    public void writeOpaqueByteArrayBlockWithOffset(final String objectPath,
            final HDF5OpaqueType dataType, final byte[] data, final int dataSize, final long offset);

    // *********************
    // Boolean
    // *********************

    /**
     * Sets a <code>boolean</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void setBooleanAttribute(final String objectPath, final String name, final boolean value);

    /**
     * Writes out a <code>boolean</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value of the data set.
     */
    public void writeBoolean(final String objectPath, final boolean value);

    /**
     * Writes out a bit field ((which can be considered the equivalent to a boolean array of rank
     * 1), provided as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by this method cannot be read
     * back by {@link IHDF5LongReader#readArray(String)} but will throw a
     * {@link ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeBitField(final String objectPath, final BitSet data);

    /**
     * Writes out a bit field ((which can be considered the equivalent to a boolean array of rank
     * 1), provided as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by this method cannot be read
     * back by {@link IHDF5LongReader#readArray(String)} but will throw a
     * {@link ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void writeBitField(final String objectPath, final BitSet data,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a bit field (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size (in 64 bit words) of the bit field to create. This will be the total
     *            size for non-extendable data sets and the size of one chunk for extendable
     *            (chunked) data sets. For extendable data sets the initial size of the array will
     *            be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void createBitField(final String objectPath, final int size);

    /**
     * Creates a bit field (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size (in 64 bit words) of the bit field to create. When using extendable data
     *            sets ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no
     *            data set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void createBitField(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a bit field array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size (in 64 bit words) of the bit field to create. This will be the total
     *            size for non-extendable data sets and the size of one chunk for extendable
     *            (chunked) data sets. For extendable data sets the initial size of the array will
     *            be 0, see {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void createBitField(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a bit field (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size (in 64 bit words) of the bit field to create. When using extendable data
     *            sets ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no
     *            data set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void createBitField(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a bit field (of rank 1). The data set needs to have been created by
     * {@link #createBitField(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createBitField(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code>
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void writeBitFieldBlock(final String objectPath, final BitSet data, final int dataSize,
            final long blockNumber);

    /**
     * Writes out a block of a <code>long</code> array (of rank 1). The data set needs to have been
     * created by {@link #createBitField(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeBitFieldBlock(String, BitSet, int, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createBitField(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code>
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#bool()}.
     */
    @Deprecated
    public void writeBitFieldBlockWithOffset(final String objectPath, BitSet data,
            final int dataSize, final long offset);

    // *********************
    // Byte
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>byte</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void setByteAttribute(final String objectPath, final String name, final byte value);

    /**
     * Set a <code>byte[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void setByteArrayAttribute(final String objectPath, final String name, final byte[] value);

    /**
     * Set a multi-dimensional code>byte</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void setByteMDArrayAttribute(final String objectPath, final String name,
            final MDByteArray value);

    /**
     * Set a <code>byte[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void setByteMatrixAttribute(final String objectPath, final String name,
            final byte[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>byte</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByte(final String objectPath, final byte value);

    /**
     * Writes out a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeByteArray(final String objectPath, final byte[] data);

    /**
     * Writes out a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteArray(final String objectPath, final byte[] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteArray(final String objectPath, final int size);

    /**
     * Creates a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>byte</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>byte</code> array (of rank 1). The data set needs to have been
     * created by {@link #createByteArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createByteArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteArrayBlock(final String objectPath, final byte[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>byte</code> array (of rank 1). The data set needs to have been
     * created by {@link #createByteArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeByteArrayBlock(String, byte[], long)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createByteArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteArrayBlockWithOffset(final String objectPath, final byte[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>byte</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMatrix(final String objectPath, final byte[][] data);

    /**
     * Writes out a <code>byte</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMatrix(final String objectPath, final byte[][] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>byte</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMatrix(final String objectPath, final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>byte</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the byte matrix to create.
     * @param sizeY The size of the y dimension of the byte matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>byte</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the byte matrix to create.
     * @param sizeY The size of the y dimension of the byte matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>byte</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMatrixBlock(final String objectPath, final byte[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>byte</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeByteMatrixBlock(String, byte[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMatrixBlockWithOffset(final String objectPath, final byte[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>byte</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeByteMatrixBlock(String, byte[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createByteMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMatrixBlockWithOffset(final String objectPath, final byte[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMDArray(final String objectPath, final MDByteArray data);

    /**
     * Writes out a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMDArray(final String objectPath, final MDByteArray data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the byte array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void createByteMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMDArrayBlock(final String objectPath, final MDByteArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray data,
            final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>byte</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int8()}.
     */
    @Deprecated
    public void writeByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

    // *********************
    // Short
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>short</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void setShortAttribute(final String objectPath, final String name, final short value);

    /**
     * Set a <code>short[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void setShortArrayAttribute(final String objectPath, final String name,
            final short[] value);

    /**
     * Set a multi-dimensional code>short</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void setShortMDArrayAttribute(final String objectPath, final String name,
            final MDShortArray value);

    /**
     * Set a <code>short[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void setShortMatrixAttribute(final String objectPath, final String name,
            final short[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>short</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShort(final String objectPath, final short value);

    /**
     * Writes out a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortArray(final String objectPath, final short[] data);

    /**
     * Writes out a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortArray(final String objectPath, final short[] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the short array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortArray(final String objectPath, final int size);

    /**
     * Creates a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the short array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the short array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>short</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the short array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>short</code> array (of rank 1). The data set needs to have been
     * created by {@link #createShortArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createShortArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortArrayBlock(final String objectPath, final short[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>short</code> array (of rank 1). The data set needs to have been
     * created by {@link #createShortArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeShortArrayBlock(String, short[], long)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createShortArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortArrayBlockWithOffset(final String objectPath, final short[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>short</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMatrix(final String objectPath, final short[][] data);

    /**
     * Writes out a <code>short</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMatrix(final String objectPath, final short[][] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>short</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMatrix(final String objectPath, final int blockSizeX,
            final int blockSizeY);

    /**
     * Creates a <code>short</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the short matrix to create.
     * @param sizeY The size of the y dimension of the short matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>short</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the short matrix to create.
     * @param sizeY The size of the y dimension of the short matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>short</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMatrixBlock(final String objectPath, final short[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>short</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeShortMatrixBlock(String, short[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMatrixBlockWithOffset(final String objectPath, final short[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>short</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeShortMatrixBlock(String, short[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createShortMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMatrixBlockWithOffset(final String objectPath, final short[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMDArray(final String objectPath, final MDShortArray data);

    /**
     * Writes out a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMDArray(final String objectPath, final MDShortArray data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the short array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void createShortMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMDArrayBlock(final String objectPath, final MDShortArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMDArrayBlockWithOffset(final String objectPath, final MDShortArray data,
            final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>short</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int16()}.
     */
    @Deprecated
    public void writeShortMDArrayBlockWithOffset(final String objectPath, final MDShortArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

    // *********************
    // Int
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>int</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void setIntAttribute(final String objectPath, final String name, final int value);

    /**
     * Set a <code>int[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void setIntArrayAttribute(final String objectPath, final String name, final int[] value);

    /**
     * Set a multi-dimensional code>int</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void setIntMDArrayAttribute(final String objectPath, final String name,
            final MDIntArray value);

    /**
     * Set a <code>int[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void setIntMatrixAttribute(final String objectPath, final String name,
            final int[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>int</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     */
    public void writeInt(final String objectPath, final int value);

    /**
     * Writes out a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeIntArray(final String objectPath, final int[] data);

    /**
     * Writes out a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntArray(final String objectPath, final int[] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the int array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntArray(final String objectPath, final int size);

    /**
     * Creates a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the int array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the int array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>int</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the int array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>int</code> array (of rank 1). The data set needs to have been
     * created by {@link #createIntArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createIntArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntArrayBlock(final String objectPath, final int[] data, final long blockNumber);

    /**
     * Writes out a block of a <code>int</code> array (of rank 1). The data set needs to have been
     * created by {@link #createIntArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeIntArrayBlock(String, int[], long)} if the total size
     * of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createIntArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntArrayBlockWithOffset(final String objectPath, final int[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>int</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeIntMatrix(final String objectPath, final int[][] data);

    /**
     * Writes out a <code>int</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMatrix(final String objectPath, final int[][] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>int</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMatrix(final String objectPath, final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>int</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the int matrix to create.
     * @param sizeY The size of the y dimension of the int matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>int</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the int matrix to create.
     * @param sizeY The size of the y dimension of the int matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>int</code> matrix (array of rank 2). The data set needs to have
     * been created by
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMatrixBlock(final String objectPath, final int[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>int</code> matrix (array of rank 2). The data set needs to have
     * been created by
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeIntMatrixBlock(String, int[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMatrixBlockWithOffset(final String objectPath, final int[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>int</code> matrix (array of rank 2). The data set needs to have
     * been created by
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeIntMatrixBlock(String, int[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createIntMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMatrixBlockWithOffset(final String objectPath, final int[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMDArray(final String objectPath, final MDIntArray data);

    /**
     * Writes out a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMDArray(final String objectPath, final MDIntArray data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the int array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void createIntMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMDArrayBlock(final String objectPath, final MDIntArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray data,
            final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>int</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int32()}.
     */
    @Deprecated
    public void writeIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

    // *********************
    // Long
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>long</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void setLongAttribute(final String objectPath, final String name, final long value);

    /**
     * Set a <code>long[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void setLongArrayAttribute(final String objectPath, final String name, final long[] value);

    /**
     * Set a multi-dimensional code>long</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void setLongMDArrayAttribute(final String objectPath, final String name,
            final MDLongArray value);

    /**
     * Set a <code>long[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void setLongMatrixAttribute(final String objectPath, final String name,
            final long[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>long</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     */
    public void writeLong(final String objectPath, final long value);

    /**
     * Writes out a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeLongArray(final String objectPath, final long[] data);

    /**
     * Writes out a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongArray(final String objectPath, final long[] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the long array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongArray(final String objectPath, final int size);

    /**
     * Creates a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the long array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the long array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>long</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the long array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongArray(final String objectPath, final long size, final int blockSize,
            final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>long</code> array (of rank 1). The data set needs to have been
     * created by {@link #createLongArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createLongArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongArrayBlock(final String objectPath, final long[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>long</code> array (of rank 1). The data set needs to have been
     * created by {@link #createLongArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeLongArrayBlock(String, long[], long)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createLongArray(String, long, int, HDF5IntStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>long</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeLongMatrix(final String objectPath, final long[][] data);

    /**
     * Writes out a <code>long</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMatrix(final String objectPath, final long[][] data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a <code>long</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMatrix(final String objectPath, final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>long</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the long matrix to create.
     * @param sizeY The size of the y dimension of the long matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>long</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the long matrix to create.
     * @param sizeY The size of the y dimension of the long matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a <code>long</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMatrixBlock(final String objectPath, final long[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>long</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeLongMatrixBlock(String, long[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMatrixBlockWithOffset(final String objectPath, final long[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>long</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeLongMatrixBlock(String, long[][], long, long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createLongMatrix(String, long, long, int, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMatrixBlockWithOffset(final String objectPath, final long[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMDArray(final String objectPath, final MDLongArray data);

    /**
     * Writes out a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMDArray(final String objectPath, final MDLongArray data,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the long array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void createLongMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMDArrayBlock(final String objectPath, final MDLongArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMDArrayBlockWithOffset(final String objectPath, final MDLongArray data,
            final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>long</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#int64()}.
     */
    @Deprecated
    public void writeLongMDArrayBlockWithOffset(final String objectPath, final MDLongArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

    // *********************
    // Float
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>float</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void setFloatAttribute(final String objectPath, final String name, final float value);

    /**
     * Set a <code>float[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void setFloatArrayAttribute(final String objectPath, final String name,
            final float[] value);

    /**
     * Set a multi-dimensional code>float</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void setFloatMDArrayAttribute(final String objectPath, final String name,
            final MDFloatArray value);

    /**
     * Set a <code>float[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void setFloatMatrixAttribute(final String objectPath, final String name,
            final float[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>float</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     */
    public void writeFloat(final String objectPath, final float value);

    /**
     * Writes out a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeFloatArray(final String objectPath, final float[] data);

    /**
     * Writes out a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatArray(final String objectPath, final float[] data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatArray(final String objectPath, final int size);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5FloatStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatArray(final String objectPath, final int size,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is
     *            <code>HDF5FloatStorageFeature.FLOATNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatArray(final String objectPath, final long size, final int blockSize,
            final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have been
     * created by {@link #createFloatArray(String, long, int, HDF5FloatStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createFloatArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatArrayBlock(final String objectPath, final float[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have been
     * created by {@link #createFloatArray(String, long, int, HDF5FloatStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatArrayBlock(String, float[], long)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatArrayBlockWithOffset(final String objectPath, final float[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeFloatMatrix(final String objectPath, final float[][] data);

    /**
     * Writes out a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMatrix(final String objectPath, final float[][] data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMatrix(final String objectPath, final int blockSizeX,
            final int blockSizeY);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the float matrix to create.
     * @param sizeY The size of the y dimension of the float matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the float matrix to create.
     * @param sizeY The size of the y dimension of the float matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMatrixBlock(final String objectPath, final float[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMatrixBlockWithOffset(final String objectPath, final float[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMatrixBlockWithOffset(final String objectPath, final float[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMDArray(final String objectPath, final MDFloatArray data);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMDArray(final String objectPath, final MDFloatArray data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the float array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5FloatStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMDArray(final String objectPath, final int[] dimensions,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void createFloatMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMDArrayBlock(final String objectPath, final MDFloatArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMDArrayBlockWithOffset(final String objectPath, final MDFloatArray data,
            final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float32()}.
     */
    @Deprecated
    public void writeFloatMDArrayBlockWithOffset(final String objectPath, final MDFloatArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

    // *********************
    // Double
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a <code>double</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void setDoubleAttribute(final String objectPath, final String name, final double value);

    /**
     * Set a <code>double[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void setDoubleArrayAttribute(final String objectPath, final String name,
            final double[] value);

    /**
     * Set a multi-dimensional code>double</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void setDoubleMDArrayAttribute(final String objectPath, final String name,
            final MDDoubleArray value);

    /**
     * Set a <code>double[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void setDoubleMatrixAttribute(final String objectPath, final String name,
            final double[][] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>double</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     */
    public void writeDouble(final String objectPath, final double value);

    /**
     * Writes out a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeDoubleArray(final String objectPath, final double[] data);

    /**
     * Writes out a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleArray(final String objectPath, final double[] data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the double array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleArray(final String objectPath, final int size);

    /**
     * Creates a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the double array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the double array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5FloatStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleArray(final String objectPath, final int size,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>double</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the double array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is
     *            <code>HDF5FloatStorageFeature.FLOATNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleArray(final String objectPath, final long size, final int blockSize,
            final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>double</code> array (of rank 1). The data set needs to have
     * been created by {@link #createDoubleArray(String, long, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createDoubleArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleArrayBlock(final String objectPath, final double[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>double</code> array (of rank 1). The data set needs to have
     * been created by {@link #createDoubleArray(String, long, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeDoubleArrayBlock(String, double[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createDoubleArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleArrayBlockWithOffset(final String objectPath, final double[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a <code>double</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeDoubleMatrix(final String objectPath, final double[][] data);

    /**
     * Writes out a <code>double</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMatrix(final String objectPath, final double[][] data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>double</code> matrix (array of rank 2). The initial size of the matrix is 0.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMatrix(final String objectPath, final int blockSizeX,
            final int blockSizeY);

    /**
     * Creates a <code>double</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the double matrix to create.
     * @param sizeY The size of the y dimension of the double matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY);

    /**
     * Creates a <code>double</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the double matrix to create.
     * @param sizeY The size of the y dimension of the double matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>double</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMatrixBlock(final String objectPath, final double[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>double</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeDoubleMatrixBlock(String, double[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMatrixBlockWithOffset(final String objectPath, final double[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>double</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeDoubleMatrixBlock(String, double[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createDoubleMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that
     * was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param dataSizeX The (real) size of <code>data</code> along the x axis (needs to be
     *            <code><= data.length</code> )
     * @param dataSizeY The (real) size of <code>data</code> along the y axis (needs to be
     *            <code><= data[0].length</code> )
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMatrixBlockWithOffset(final String objectPath, final double[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeDoubleMDArray(final String objectPath, final MDDoubleArray data);

    /**
     * Writes out a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMDArray(final String objectPath, final MDDoubleArray data,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the double array to create. This will be the total
     *            dimensions for non-extendable data sets and the dimensions of one chunk (extent
     *            along each axis) for extendable (chunked) data sets. For extendable data sets the
     *            initial size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array. Will be the total dimensions for
     *            non-extendable data sets and the dimensions of one chunk for extendable (chunked)
     *            data sets For extendable data sets the initial size of the array along each axis
     *            will be 0, see {@link HDF5FloatStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMDArray(final String objectPath, final int[] dimensions,
            final HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void createDoubleMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMDArrayBlock(final String objectPath, final MDDoubleArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMDArrayBlockWithOffset(final String objectPath,
            final MDDoubleArray data, final long[] offset);

    /**
     * Writes out a block of a multi-dimensional <code>double</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#float64()}.
     */
    @Deprecated
    public void writeDoubleMDArrayBlockWithOffset(final String objectPath,
            final MDDoubleArray data, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset);

    // *********************
    // String
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Sets a string attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringAttribute(final String objectPath, final String name, final String value);

    /**
     * Sets a string attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @param maxLength The maximal length of the value.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringAttribute(final String objectPath, final String name, final String value,
            final int maxLength);

    /**
     * Sets a string array attribute on the referenced object. The length of the array is taken to
     * be the longest string in <var>value</var>.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringArrayAttribute(final String objectPath, final String name,
            final String[] value);

    /**
     * Sets a string array attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @param maxLength The maximal length of any element in <var>value</var>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringArrayAttribute(final String objectPath, final String name,
            final String[] value, final int maxLength);

    /**
     * Sets a multi-dimensional string array attribute on the referenced object. The length of the
     * array is taken to be the longest string in <var>value</var>.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringMDArrayAttribute(final String objectPath, final String name,
            final MDArray<String> value);

    /**
     * Sets a multi-dimensional string array attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @param maxLength The maximal length of the value.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringMDArrayAttribute(final String objectPath, final String name,
            final MDArray<String> value, final int maxLength);

    /**
     * Sets a string attribute with variable length on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void setStringAttributeVariableLength(final String objectPath, final String name,
            final String value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of the <var>data</var>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeString(final String objectPath, final String data, final int maxLength);

    /**
     * Writes out a <code>String</code> with a fixed maximal length (which is the length of the
     * string <var>data</var>).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeString(final String objectPath, final String data);

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeString(final String objectPath, final String data,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a <code>String</code> with a fixed maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of the <var>data</var>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeString(final String objectPath, final String data, final int maxLength,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a <code>String</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringArray(final String objectPath, final String[] data,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is defined by the longest string in <var>data</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeStringArray(final String objectPath, final String[] data);

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringArray(final String objectPath, final String[] data, final int maxLength);

    /**
     * Writes out a <code>String</code> array (of rank 1). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringArray(final String objectPath, final String[] data, final int maxLength,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * fixed maximal length which is defined by the longest string in <var>data</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArray(final String objectPath, final MDArray<String> data);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * fixed maximal length which is defined by the longest string in <var>data</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArray(final String objectPath, final MDArray<String> data,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArray(final String objectPath, final MDArray<String> data,
            final int maxLength);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * fixed maximal length which is given by <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param maxLength The maximal length of any of the strings in <var>data</var>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArray(final String objectPath, final MDArray<String> data,
            final int maxLength, final HDF5GenericStorageFeatures features);

    /**
     * Creates a <code>String</code> array (of rank 1) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringArray(final String objectPath, final int maxLength, final int size);

    /**
     * Creates a <code>String</code> array (of rank 1) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param size The size of the String array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize);

    /**
     * Creates a <code>String</code> array (of rank 1) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringArray(final String objectPath, final int maxLength, final int size,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a <code>String</code> array (of rank 1) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param size The size of the String array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringArray(final String objectPath, final int maxLength, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features);

    /**
     * Writes out a block of a <code>String</code> array (of rank 1). The data set needs to have
     * been created by
     * {@link #createStringArray(String, int, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createStringArray(String, int, long, int, HDF5GenericStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringArrayBlock(final String objectPath, final String[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>String</code> array (of rank 1). The data set needs to have
     * been created by
     * {@link #createStringArray(String, int, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeStringArrayBlock(String, String[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createStringArray(String, int, long, int, HDF5GenericStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringArrayBlockWithOffset(final String objectPath, final String[] data,
            final int dataSize, final long offset);

    /**
     * Creates a <code>String</code> array (of rank N) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param dimensions The size of the String array to create. When using extendable data sets
     *            ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data
     *            set smaller than this size can be created, however data sets may be larger.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringMDArray(final String objectPath, final int maxLength,
            final int[] dimensions);

    /**
     * Creates a <code>String</code> array (of rank N) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param dimensions The size of the String array to create. When using extendable data sets
     *            ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data
     *            set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block in each dimension (for block-wise IO). Ignored if no
     *            extendable data sets are used (see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringMDArray(final String objectPath, final int maxLength,
            final long[] dimensions, final int[] blockSize);

    /**
     * Creates a <code>String</code> array (of rank N) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param dimensions The size of the String array to create. When using extendable data sets
     *            ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data
     *            set smaller than this size can be created, however data sets may be larger.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringMDArray(final String objectPath, final int maxLength,
            final int[] dimensions, final HDF5GenericStorageFeatures features);

    /**
     * Creates a <code>String</code> array (of rank N) for Strings of length <var>maxLength</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param maxLength The maximal length of one String in the array.
     * @param dimensions The size of the String array to create. When using extendable data sets
     *            ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data
     *            set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block in each dimension (for block-wise IO). Ignored if no
     *            extendable data sets are used (see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringMDArray(final String objectPath, final int maxLength,
            final long[] dimensions, final int[] blockSize,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a block of a <code>String</code> array (of rank N). The data set needs to have
     * been created by
     * {@link #createStringMDArray(String, int, long[], int[], HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createStringMDArray(String, int, long[], int[], HDF5GenericStorageFeatures)} call
     * that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArrayBlock(final String objectPath, final MDArray<String> data,
            final long[] blockNumber);

    /**
     * Writes out a block of a <code>String</code> array (of rank N). The data set needs to have
     * been created by
     * {@link #createStringMDArray(String, int, long[], int[], HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createStringMDArray(String, int, long[], int[], HDF5GenericStorageFeatures)} call
     * that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringMDArrayBlockWithOffset(final String objectPath,
            final MDArray<String> data, final long[] offset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringVariableLength(final String objectPath, final String data);

    /**
     * Writes out a <code>String[]</code> where each String of the array has a variable maximal
     * length.
     * <p>
     * The advantage of this method over {@link #writeStringArray(String, String[])} is that when
     * writing a new string later it can have a different (also greater) length. The disadvantage is
     * that it it is more time consuming to read and write this kind of string and that it can't be
     * compressed.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringVariableLengthArray(final String objectPath, final String[] data);

    /**
     * Writes out a <code>String[]</code> where each String of the array has a variable maximal
     * length.
     * <p>
     * The advantage of this method over {@link #writeStringArray(String, String[])} is that when
     * writing a new string later it can have a different (also greater) length. The disadvantage is
     * that it it is more time consuming to read and write this kind of string and that it can't be
     * compressed.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringVariableLengthArray(final String objectPath, final String[] data,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a <code>String[]</code> where each String of the array has a variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthArray(final String objectPath, final int size);

    /**
     * Creates a <code>String[]</code> where each String of the array has a variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The intial size of the array.
     * @param blockSize The size of block in the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize);

    /**
     * Creates a <code>String[]</code> where each String of the array has a variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The initial size of the array.
     * @param blockSize The size of block in the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthArray(final String objectPath, final long size,
            final int blockSize, final HDF5GenericStorageFeatures features);

    /**
     * Creates a <code>String[]</code> where each String of the array has a variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>String</code> array where each String of the array has a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The initial dimensions (along each axis) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthMDArray(final String objectPath, final int[] dimensions,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>String</code> array where each String of the array has a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The initial dimensions (along each axis) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional <code>String</code> array where each String of the array has a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The initial dimensions (along each axis) of the array.
     * @param blockSize The size of a contiguously stored block (along each axis) in the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthMDArray(final String objectPath, final long[] dimensions,
            final int[] blockSize, final HDF5GenericStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>String</code> array where each String of the array has a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The initial dimensions (along each axis) of the array.
     * @param blockSize The size of a contiguously stored block (along each axis) in the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void createStringVariableLengthMDArray(final String objectPath, final long[] dimensions,
            final int[] blockSize);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringVariableLengthMDArray(final String objectPath, final MDArray<String> data);

    /**
     * Writes out a <code>String</code> array (of rank N). Each element of the array will have a
     * variable maximal length.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#string()} instead.
     */
    @Deprecated
    public void writeStringVariableLengthMDArray(final String objectPath,
            final MDArray<String> data, final HDF5GenericStorageFeatures features);

    // *********************
    // Date & Time
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Set a date value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param date The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setDateAttribute(final String objectPath, final String attributeName,
            final Date date);

    /**
     * Set a date array value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param dates The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setDateArrayAttribute(final String objectPath, final String attributeName,
            final Date[] dates);

    /**
     * Set a time stamp value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeStamp The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setTimeStampAttribute(final String objectPath, final String attributeName,
            final long timeStamp);

    /**
     * Set a time stamp array value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeStamps The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setTimeStampArrayAttribute(final String objectPath, final String attributeName,
            final long[] timeStamps);

    /**
     * Set a time duration value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDuration The value of the attribute.
     * @param timeUnit The unit of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setTimeDurationAttribute(final String objectPath, final String attributeName,
            final long timeDuration, final HDF5TimeUnit timeUnit);

    /**
     * Set a time duration value as attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDuration The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setTimeDurationAttribute(final String objectPath, final String attributeName,
            final HDF5TimeDuration timeDuration);

    /**
     * Set a time duration array value as attribute on the referenced object. The smallest time unit
     * in <var>timeDurations</var> will be used as the time unit of the array.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * <p>
     * <em>Note: Time durations are stored as a <code>long[]</code> array.</em>
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param attributeName The name of the attribute.
     * @param timeDurations The value of the attribute.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void setTimeDurationArrayAttribute(final String objectPath, final String attributeName,
            final HDF5TimeDurationArray timeDurations);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a time stamp value. The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamp The timestamp to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeStamp(final String objectPath, final long timeStamp);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final int size);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The length of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final long size, final int blockSize,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a time stamp array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeStampArray(final String objectPath, final int size,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeStampArray(final String objectPath, final long[] timeStamps);

    /**
     * Writes out a time stamp array (of rank 1). The data set will be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * <p>
     * <em>Note: Time stamps are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeStamps The timestamps to write as number of milliseconds since January 1, 1970,
     *            00:00:00 GMT.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeStampArray(final String objectPath, final long[] timeStamps,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link IHDF5LongWriter#createArray(String, long, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeStampArrayBlock(final String objectPath, final long[] data,
            final long blockNumber);

    /**
     * Writes out a block of a time stamp array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeStampArray(String, long, int, HDF5GenericStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeTimeStampArrayBlock(String, long[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link IHDF5LongWriter#createArray(String, long, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeStampArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset);

    /**
     * Writes out a time stamp value provided as a {@link Date}.
     * <p>
     * <em>Note: The time stamp is stored as <code>long</code> array and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param date The date to write.
     * @see #writeTimeStamp(String, long)
     */
    public void writeDate(final String objectPath, final Date date);

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time stamps are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @see #writeTimeStampArray(String, long[])
     */
    public void writeDateArray(final String objectPath, final Date[] dates);

    /**
     * Writes out a {@link Date} array (of rank 1).
     * <p>
     * <em>Note: Time date is stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dates The dates to write.
     * @param features The storage features of the data set.
     * @see #writeTimeStampArray(String, long[], HDF5GenericStorageFeatures)
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeDateArray(final String objectPath, final Date[] dates,
            final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time duration value in seconds.
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in seconds.
     * @deprecated Use {@link IHDF5TimeDurationWriter#write(String, HDF5TimeDuration)} instead.
     */
    @Deprecated
    public void writeTimeDuration(final String objectPath, final long timeDuration);

    /**
     * Writes out a time duration value.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeDuration(final String objectPath, final long timeDuration,
            final HDF5TimeUnit timeUnit);

    /**
     * Writes out a time duration value.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDuration The duration of time to write.
     */
    public void writeTimeDuration(final String objectPath, final HDF5TimeDuration timeDuration);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long</code> values.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the byte array to create. This will be the total size for
     *            non-extendable data sets and the size of one chunk for extendable (chunked) data
     *            sets. For extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final int size,
            final HDF5TimeUnit timeUnit);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the data set to create.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>deflate == false</code>.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final long size,
            final int blockSize, final HDF5TimeUnit timeUnit,
            final HDF5GenericStorageFeatures features);

    /**
     * Creates a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5GenericStorageFeatures}.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void createTimeDurationArray(final String objectPath, final int size,
            final HDF5TimeUnit timeUnit, final HDF5GenericStorageFeatures features);

    /**
     * Writes out a time duration array in seconds (of rank 1).
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in seconds.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)}
     *             instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)}
     *             instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit);

    /**
     * Writes out a time duration array (of rank 1). The smallest time unit in
     * <var>timeDurations</var> will be used as the time unit of the array.
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>. The array
     *            will be stored in the smallest time unit, durations given in larger time units
     *            will be converted.
     * @deprecated Use {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray)} and
     *             {@link HDF5TimeDurationArray#create(HDF5TimeDuration...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>.
     * @param timeUnit The unit of the time duration.
     * @param features The storage features of the data set.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray, HDF5IntStorageFeatures)
     *             )} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath, final long[] timeDurations,
            final HDF5TimeUnit timeUnit, final HDF5IntStorageFeatures features);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write in the given <var>timeUnit</var>. The array
     *            will be stored in the smallest time unit, durations given in larger time units
     *            will be converted.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArray(String, HDF5TimeDurationArray, HDF5IntStorageFeatures)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDuration[] timeDurations, final HDF5IntStorageFeatures features);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write.
     */
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDurationArray timeDurations);

    /**
     * Writes out a time duration array (of rank 1).
     * <p>
     * <em>Note: Time durations are stored as <code>long[]</code> arrays and tagged as the according
     * type variant.</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeDurations The time durations to write.
     * @param features The storage features used to store the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeDurationArray(final String objectPath,
            final HDF5TimeDurationArray timeDurations, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a time duration array. The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath,
            final HDF5TimeDurationArray data, final long blockNumber);

    /**
     * Writes out a block of a time duration array. The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#time()} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDurationArray data, final int dataSize, final long offset);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlock(String, HDF5TimeDurationArray, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeUnit, long...)} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath, final long[] data,
            final long blockNumber, final HDF5TimeUnit timeUnit);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlockWithOffset(String, HDF5TimeDurationArray, int, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeUnit, long[])} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath, final long[] data,
            final int dataSize, final long offset, final HDF5TimeUnit timeUnit);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlock(String, HDF5TimeDurationArray, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public void writeTimeDurationArrayBlock(final String objectPath, final HDF5TimeDuration[] data,
            final long blockNumber);

    /**
     * Writes out a block of a time duration array (which is stored as a <code>long</code> array of
     * rank 1). The data set needs to have been created by
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of
     * {@link #writeTimeDurationArrayBlock(String, long[], long, HDF5TimeUnit)} if the total size of
     * the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createTimeDurationArray(String, long, int, HDF5TimeUnit, HDF5GenericStorageFeatures)}
     * call that was used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use
     *             {@link IHDF5TimeDurationWriter#writeArrayBlockWithOffset(String, HDF5TimeDurationArray, int, long)}
     *             and {@link HDF5TimeDurationArray#create(HDF5TimeDuration[])}.
     */
    @Deprecated
    public void writeTimeDurationArrayBlockWithOffset(final String objectPath,
            final HDF5TimeDuration[] data, final int dataSize, final long offset);

    // *********************
    // Reference
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Sets an object reference attribute to the referenced object.
     * <p>
     * Both the object referenced with <var>objectPath</var> and <var>referencedObjectPath</var>
     * must exist, that is it need to have been written before by one of the <code>write()</code> or
     * <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param referencedObjectPath The path of the object to reference.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void setObjectReferenceAttribute(final String objectPath, final String name,
            final String referencedObjectPath);

    /**
     * Sets a 1D object reference array attribute to referenced objects.
     * <p>
     * Both the object referenced with <var>objectPath</var> and all
     * <var>referencedObjectPaths</var> must exist, that is it need to have been written before by
     * one of the <code>write()</code> or <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param referencedObjectPaths The paths of the objects to reference.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void setObjectReferenceArrayAttribute(final String objectPath, final String name,
            final String[] referencedObjectPaths);

    /**
     * Sets an object reference array attribute to referenced objects.
     * <p>
     * Both the object referenced with <var>objectPath</var> and all
     * <var>referencedObjectPaths</var> must exist, that is it need to have been written before by
     * one of the <code>write()</code> or <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param referencedObjectPaths The paths of the objects to reference.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void setObjectReferenceMDArrayAttribute(final String objectPath, final String name,
            final MDArray<String> referencedObjectPaths);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes an object reference to the referenced object.
     * <p>
     * The object referenced with <var>referencedObjectPath</var> must exist, that is it need to
     * have been written before by one of the <code>write()</code> or <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to write.
     * @param referencedObjectPath The path of the object to reference.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReference(String objectPath, String referencedObjectPath);

    /**
     * Writes an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPath The names of the object to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath);

    /**
     * Writes an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPath The names of the object to write.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath, final HDF5IntStorageFeatures features);

    /**
     * Creates an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceArray(final String objectPath, final int size);

    /**
     * Creates an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceArray(final String objectPath, final long size,
            final int blockSize);

    /**
     * Creates an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. This will be the total size for non-extendable
     *            data sets and the size of one chunk for extendable (chunked) data sets. For
     *            extendable data sets the initial size of the array will be 0, see
     *            {@link HDF5IntStorageFeatures}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceArray(final String objectPath, final int size,
            final HDF5IntStorageFeatures features);

    /**
     * Creates an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the array to create. When using extendable data sets ((see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data set
     *            smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data
     *            sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})
     *            and <code>features</code> is <code>HDF5IntStorageFeature.INTNO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceArray(final String objectPath, final long size,
            final int blockSize, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of an array (of rank 1) of object references. The data set needs to have
     * been created by
     * {@link #createObjectReferenceArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createObjectReferenceArray(String, long, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The paths of the referenced objects to write. The length defines
     *            the block size. Must not be <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceArrayBlock(final String objectPath,
            final String[] referencedObjectPaths, final long blockNumber);

    /**
     * Writes out a block of an array (of rank 1) of object references. The data set needs to have
     * been created by
     * {@link #createObjectReferenceArray(String, long, int, HDF5IntStorageFeatures)} beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createObjectReferenceArray(String, long, int, HDF5IntStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The paths of the referenced objects to write. The length defines
     *            the block size. Must not be <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceArrayBlockWithOffset(final String objectPath,
            final String[] referencedObjectPaths, final int dataSize, final long offset);

    /**
     * Writes an array (of rank N) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The names of the object to write.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceMDArray(final String objectPath,
            final MDArray<String> referencedObjectPaths);

    /**
     * Writes an array (of rank N) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The names of the object to write.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceMDArray(final String objectPath,
            final MDArray<String> referencedObjectPaths, final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array to create. This will be the total dimensions
     *            for non-extendable data sets and the dimensions of one chunk (extent along each
     *            axis) for extendable (chunked) data sets. For extendable data sets the initial
     *            size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceMDArray(final String objectPath, final int[] dimensions);

    /**
     * Creates a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array to create. This will be the total dimensions
     *            for non-extendable data sets and the dimensions of one chunk (extent along each
     *            axis) for extendable (chunked) data sets. For extendable data sets the initial
     *            size of the array along each axis will be 0, see
     *            {@link ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator#dontUseExtendableDataTypes}.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceMDArray(final String objectPath, final int[] dimensions,
            final HDF5IntStorageFeatures features);

    /**
     * Creates a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void createObjectReferenceMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5IntStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The paths of the object references to write. Must not be
     *            <code>null</code>. All columns need to have the same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceMDArrayBlock(final String objectPath,
            final MDArray<String> referencedObjectPaths, final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The paths of the object references to write. Must not be
     *            <code>null</code>.
     * @param offset The offset in the data set to start writing to in each dimension.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceMDArrayBlockWithOffset(final String objectPath,
            final MDArray<String> referencedObjectPaths, final long[] offset);

    /**
     * Writes out a block of a multi-dimensional array of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The paths of the object references to write. Must not be
     *            <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     * @deprecated Use the corresponding method in {@link IHDF5Writer#reference()}.
     */
    @Deprecated
    public void writeObjectReferenceMDArrayBlockWithOffset(final String objectPath,
            final MDLongArray referencedObjectPaths, final int[] blockDimensions,
            final long[] offset, final int[] memoryOffset);

    // *********************
    // Enums
    // *********************

    /**
     * Returns the full writer for enums.
     * 
     * @deprecated Use {@link IHDF5Writer#enumeration()} instead.
     */
    @Deprecated
    public IHDF5EnumWriter enums();

    // *********************
    // Compounds
    // *********************

    /**
     * Returns the full writer for compounds.
     * 
     * @deprecated Use {@link IHDF5Writer#compound()} instead.
     */
    @Deprecated
    public IHDF5CompoundWriter compounds();

}
