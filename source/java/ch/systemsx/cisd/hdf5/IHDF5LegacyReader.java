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

import java.util.BitSet;
import java.util.Date;

import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;

/**
 * The legacy interface for reading HDF5 files. Do not use in any new code as it will be removed in
 * a future version of JHDF5.
 * 
 * @author Bernd Rinn
 */
@Deprecated
public interface IHDF5LegacyReader extends IHDF5EnumBasicReader, IHDF5CompoundBasicReader
{
    // *********************
    // Generic
    // *********************

    /**
     * Returns the tag of the opaque data type associated with <var>objectPath</var>, or
     * <code>null</code>, if <var>objectPath</var> is not of an opaque data type (i.e. if
     * <code>reader.getDataSetInformation(objectPath).getTypeInformation().getDataClass() != HDF5DataClass.OPAQUE</code>
     * ).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The tag of the opaque data type, or <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public String tryGetOpaqueTag(final String objectPath);

    /**
     * Returns the opaque data type or <code>null</code>, if <var>objectPath</var> is not of such a
     * data type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The opaque data type, or <code>null</code>.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public HDF5OpaqueType tryGetOpaqueType(final String objectPath);

    /**
     * Gets the (unchanged) byte array values of an attribute <var>attributeName</var> of object
     * </var>objectPath</var>.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public byte[] getAttributeAsByteArray(final String objectPath, final String attributeName);

    /**
     * Reads the data set <var>objectPath</var> as byte array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public byte[] readAsByteArray(final String objectPath);

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * <em>Must not be called for data sets of rank other than 1 and must not be called on Strings!</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size in numbers of elements (this will be the length of the
     *            <code>byte[]</code> returned, divided by the size of one element).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set is not of rank 1 or is a String.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public byte[] readAsByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1).
     * <em>Must not be called for data sets of rank other than 1 and must not be called on Strings!</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size in numbers of elements (this will be the length of the
     *            <code>byte[]</code> returned, divided by the size of one element).
     * @param offset The offset of the block to read as number of elements (starting with 0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public byte[] readAsByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset) throws HDF5JavaException;

    /**
     * Reads a block from data set <var>objectPath</var> as byte array (of rank 1) into
     * <var>buffer</var>.
     * <em>Must not be called for data sets of rank other than 1 and must not be called on Strings!</em>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param buffer The buffer to read the values in.
     * @param blockSize The block size in numbers of elements (this will be the length of the
     *            <code>byte[]</code> returned, divided by the size of one element).
     * @param offset The offset of the block in the data set as number of elements (zero-based).
     * @param memoryOffset The offset of the block in <var>buffer</var> as number of elements
     *            (zero-based).
     * @return The effective block size.
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public int readAsByteArrayToBlockWithOffset(final String objectPath, final byte[] buffer,
            final int blockSize, final long offset, final int memoryOffset)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * <em>Must not be called for data sets of rank other than 1 and must not be called on Strings!</em>
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#opaque()} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<byte[]>> getAsByteArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    // *********************
    // Boolean
    // *********************

    /**
     * Reads a <code>boolean</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @throws HDF5JavaException If the attribute is not a boolean type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#bool()}.
     */
    @Deprecated
    public boolean getBooleanAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

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
     * {@link IHDF5LongWriter#writeArray(String, long[])} cannot be read back by this method but
     * will throw a {@link HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The {@link BitSet} read from the data set.
     * @throws HDF5DatatypeInterfaceException If the <var>objectPath</var> is not of bit field type.
     */
    public BitSet readBitField(final String objectPath) throws HDF5DatatypeInterfaceException;

    /**
     * Reads a block of a bit field (which can be considered the equivalent to a boolean array of
     * rank 1) from the data set <var>objectPath</var> and returns it as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by
     * {@link IHDF5LongWriter#writeArray(String, long[])} cannot be read back by this method but
     * will throw a {@link HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block (in 64 bit words) to read.
     * @param blockNumber The number of the block to read.
     * @return The {@link BitSet} read from the data set.
     * @throws HDF5DatatypeInterfaceException If the <var>objectPath</var> is not of bit field type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#bool()}.
     */
    @Deprecated
    public BitSet readBitFieldBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block of a bit field (which can be considered the equivalent to a boolean array of
     * rank 1) from the data set <var>objectPath</var> and returns it as a Java {@link BitSet}.
     * <p>
     * Note that the storage form of the bit array is a <code>long[]</code>. However, it is marked
     * in HDF5 to be interpreted bit-wise. Thus a data set written by
     * {@link IHDF5LongWriter#writeArray(String, long[])} cannot be read back by this method but
     * will throw a {@link HDF5DatatypeInterfaceException}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block (in 64 bit words) to read.
     * @param offset The offset of the block (in 64 bit words) to start reading from.
     * @return The {@link BitSet} read from the data set.
     * @throws HDF5DatatypeInterfaceException If the <var>objectPath</var> is not of bit field type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#bool()}.
     */
    @Deprecated
    public BitSet readBitFieldBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Returns <code>true</code> if the <var>bitIndex</var> of the bit field dataset
     * <var>objectPath</var> is set, <code>false</code> otherwise.
     * <p>
     * Will also return <code>false</code>, if <var>bitIndex</var> is outside of the bitfield
     * dataset.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#bool()}.
     */
    @Deprecated
    public boolean isBitSetInBitField(final String objectPath, final int bitIndex);

    // *********************
    // Byte
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>byte</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte getByteAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>byte[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[] getByteArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>byte</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public MDByteArray getByteMDArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>byte</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[][] getByteMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>byte</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte readByte(final String objectPath);

    /**
     * Reads a <code>byte</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[] readByteArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>byte</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public int[] readToByteMDArrayWithOffset(final String objectPath, final MDByteArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>byte</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public int[] readToByteMDArrayBlockWithOffset(final String objectPath, final MDByteArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[] readByteArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[] readByteArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>byte</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[][] readByteMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[][] readByteMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public byte[][] readByteMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public MDByteArray readByteMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public MDByteArray readByteMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>byte</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public MDByteArray readByteMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<byte[]>> getByteArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int8()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDByteArray>> getByteMDArrayNaturalBlocks(
            final String dataSetPath);

    // *********************
    // Short
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>short</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short getShortAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>short[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[] getShortArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>short</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public MDShortArray getShortMDArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>short</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[][] getShortMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>short</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short readShort(final String objectPath);

    /**
     * Reads a <code>short</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[] readShortArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>short</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public int[] readToShortMDArrayWithOffset(final String objectPath, final MDShortArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>short</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public int[] readToShortMDArrayBlockWithOffset(final String objectPath,
            final MDShortArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[] readShortArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[] readShortArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>short</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[][] readShortMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[][] readShortMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public short[][] readShortMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public MDShortArray readShortMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public MDShortArray readShortMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>short</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public MDShortArray readShortMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<short[]>> getShortArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int16()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDShortArray>> getShortMDArrayNaturalBlocks(
            final String dataSetPath);

    // *********************
    // Int
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>int</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int getIntAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>int[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[] getIntArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>int</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public MDIntArray getIntMDArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>int</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[][] getIntMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>int</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public int readInt(final String objectPath);

    /**
     * Reads a <code>int</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public int[] readIntArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>int</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[] readToIntMDArrayWithOffset(final String objectPath, final MDIntArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>int</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[] readToIntMDArrayBlockWithOffset(final String objectPath, final MDIntArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[] readIntArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block from <code>int</code> array (of rank 1) from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>int[]</code> returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[] readIntArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>int</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public int[][] readIntMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[][] readIntMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public int[][] readIntMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public MDIntArray readIntMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public MDIntArray readIntMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>int</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public MDIntArray readIntMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<int[]>> getIntArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int32()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDIntArray>> getIntMDArrayNaturalBlocks(final String dataSetPath);

    // *********************
    // Long
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>long</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long getLongAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>long[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[] getLongArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>long</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public MDLongArray getLongMDArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>long</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[][] getLongMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>long</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public long readLong(final String objectPath);

    /**
     * Reads a <code>long</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public long[] readLongArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>long</code> array data set <var>objectPath</var> into a given
     * <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public int[] readToLongMDArrayWithOffset(final String objectPath, final MDLongArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>long</code> array data set <var>objectPath</var>
     * into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public int[] readToLongMDArrayBlockWithOffset(final String objectPath, final MDLongArray array,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[] readLongArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[] readLongArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>long</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public long[][] readLongMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[][] readLongMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public long[][] readLongMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public MDLongArray readLongMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public MDLongArray readLongMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>long</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public MDLongArray readLongMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<long[]>> getLongArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#int64()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDLongArray>> getLongMDArrayNaturalBlocks(
            final String dataSetPath);

    // *********************
    // Float
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>float</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float getFloatAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>float[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[] getFloatArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>float</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public MDFloatArray getFloatMDArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>float</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[][] getFloatMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>float</code> value from the data set <var>objectPath</var>. This method doesn't
     * check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public float readFloat(final String objectPath);

    /**
     * Reads a <code>float</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public float[] readFloatArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>float</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public int[] readToFloatMDArrayWithOffset(final String objectPath, final MDFloatArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>float</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public int[] readToFloatMDArrayBlockWithOffset(final String objectPath,
            final MDFloatArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[] readFloatArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[] readFloatArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>float</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public float[][] readFloatMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[][] readFloatMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public float[][] readFloatMatrixBlockWithOffset(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long offsetX, final long offsetY) throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public MDFloatArray readFloatMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public MDFloatArray readFloatMDArrayBlock(final String objectPath, final int[] blockDimensions,
            final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>float</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public MDFloatArray readFloatMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<float[]>> getFloatArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float32()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDFloatArray>> getFloatMDArrayNaturalBlocks(
            final String dataSetPath);

    // *********************
    // Double
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a <code>double</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double getDoubleAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a <code>double[]</code> attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[] getDoubleArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional array <code>double</code> attribute named <var>attributeName</var>
     * from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute array value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public MDDoubleArray getDoubleMDArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads a <code>double</code> matrix attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute matrix value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[][] getDoubleMatrixAttribute(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a <code>double</code> value from the data set <var>objectPath</var>. This method
     * doesn't check the data space but simply reads the first value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The value read from the data set.
     */
    public double readDouble(final String objectPath);

    /**
     * Reads a <code>double</code> array (of rank 1) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     */
    public double[] readDoubleArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>double</code> array data set <var>objectPath</var> into a
     * given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param memoryOffset The offset in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public int[] readToDoubleMDArrayWithOffset(final String objectPath, final MDDoubleArray array,
            final int[] memoryOffset);

    /**
     * Reads a block of the multi-dimensional <code>double</code> array data set
     * <var>objectPath</var> into a given <var>array</var> in memory.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param array The array to read the data into.
     * @param blockDimensions The size of the block to read along each axis.
     * @param offset The offset of the block in the data set.
     * @param memoryOffset The offset of the block in the array to write the data to.
     * @return The effective dimensions of the block in <var>array</var> that was filled.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public int[] readToDoubleMDArrayBlockWithOffset(final String objectPath,
            final MDDoubleArray array, final int[] blockDimensions, final long[] offset,
            final int[] memoryOffset);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[] readDoubleArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[] readDoubleArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a <code>double</code> matrix (array of arrays) from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the data set <var>objectPath</var> is not of rank 2.
     */
    public double[][] readDoubleMatrix(final String objectPath) throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[][] readDoubleMatrixBlock(final String objectPath, final int blockSizeX,
            final int blockSizeY, final long blockNumberX, final long blockNumberY)
            throws HDF5JavaException;

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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public double[][] readDoubleMatrixBlockWithOffset(final String objectPath,
            final int blockSizeX, final int blockSizeY, final long offsetX, final long offsetY)
            throws HDF5JavaException;

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public MDDoubleArray readDoubleMDArray(final String objectPath);

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public MDDoubleArray readDoubleMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a multi-dimensional <code>double</code> array from the data set <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The data block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public MDDoubleArray readDoubleMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<double[]>> getDoubleArrayNaturalBlocks(final String dataSetPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#float64()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDDoubleArray>> getDoubleMDArrayNaturalBlocks(
            final String dataSetPath);

    // *********************
    // String
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads a string attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>. Considers '\0' as end of string.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public String getStringAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a string array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>. Considers '\0' as end of string.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public String[] getStringArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a multi-dimensional string array attribute named <var>attributeName</var> from the
     * object <var>objectPath</var>. Considers '\0' as end of string.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The attribute value read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public MDArray<String> getStringMDArrayAttribute(final String objectPath,
            final String attributeName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads a string from the data set <var>objectPath</var>. Considers '\0' as end of string. This
     * needs to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String readString(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a string array (of rank 1) from the data set <var>objectPath</var>. The elements of
     * this data set need to be a string type. Considers '\0' as end of string.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     */
    public String[] readStringArray(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a block of a string array (of rank 1) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block to read from the data set.
     * @param blockNumber The number of the block to read from the data set (the offset is
     *            <code>blockSize * blockNumber</code>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public String[] readStringArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block of a string array (of rank 1) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The size of the block to read from the data set.
     * @param offset The offset of the block in the data set.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public String[] readStringArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Reads a string array (of rank N) from the data set <var>objectPath</var>. The elements of
     * this data set need to be a string type. Considers '\0' as end of string.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public MDArray<String> readStringMDArray(final String objectPath);

    /**
     * Reads a block of a string array (of rank N) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The dimensions (along each axis) of the block to read from the data
     *            set.
     * @param blockNumber The number of the block to read from the data set (the offset in each
     *            dimension i is <code>blockSize[i] * blockNumber[i]</code>).
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public MDArray<String> readStringMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a block of a string array (of rank N) from the data set <var>objectPath</var>. The
     * elements of this data set need to be a string type.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The dimensions (along each axis) of the block to read from the data
     *            set.
     * @param offset The offset of the block in the data set.
     * @return The data read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not a string type.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public MDArray<String> readStringMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Provides all natural blocks of this one-dimensional string data set to iterate over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<String[]>> getStringArrayNaturalBlocks(final String objectPath)
            throws HDF5JavaException;

    /**
     * Provides all natural blocks of this multi-dimensional string data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#string()} instead.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getStringMDArrayNaturalBlocks(
            final String objectPath);

    // *********************
    // Date & Time
    // *********************

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Returns <code>true</code>, if the attribute <var>attributeName</var> of data set
     * <var>objectPath</var> is a time stamp and <code>false</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public boolean isTimeStamp(final String objectPath, String attributeName)
            throws HDF5JavaException;

    /**
     * Reads a time stamp attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long getTimeStampAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp array; each element is a number of milliseconds since January 1, 1970,
     *         00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] getTimeStampArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var> and returns it as a <code>Date</code>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public Date getDateAttribute(final String objectPath, final String attributeName);

    /**
     * Reads a time stamp array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var> and returns it as a <code>Date[]</code>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the attribute <var>attributeName</var> of objct
     *             <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public Date[] getDateArrayAttribute(final String objectPath, final String attributeName);

    /**
     * Returns <code>true</code>, if the attribute <var>attributeName</var> of data set
     * <var>objectPath</var> is a time duration and <code>false</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public boolean isTimeDuration(final String objectPath, String attributeName)
            throws HDF5JavaException;

    /**
     * Reads a time duration attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDuration getTimeDurationAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads a time duration array attribute named <var>attributeName</var> from the data set
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The time duration.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray getTimeDurationArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Returns the time unit, if the attribute given by <var>attributeName</var> of object
     * <var>objectPath</var> is a time duration and <code>null</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath, final String attributeName)
            throws HDF5JavaException;

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time stamp and
     * <code>false</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public boolean isTimeStamp(final String objectPath) throws HDF5JavaException;

    /**
     * Returns <code>true</code>, if the data set given by <var>objectPath</var> is a time duration
     * and <code>false</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public boolean isTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Returns the time unit, if the data set given by <var>objectPath</var> is a time duration and
     * <code>null</code> otherwise.
     * 
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeUnit tryGetTimeUnit(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time stamp value from the data set <var>objectPath</var>. The time stamp is stored as
     * a <code>long</code> value in the HDF5 file. It needs to be tagged as type variant
     * {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time stamp as number of milliseconds since January 1, 1970, 00:00:00 GMT.
     * @throws HDF5JavaException If the <var>objectPath</var> is not defined as type variant
     *             {@link HDF5DataTypeVariant#TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH}.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
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
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
    public long[] readTimeStampArrayBlockWithOffset(final String objectPath, final int blockSize,
            final long offset);

    /**
     * Provides all natural blocks of this one-dimensional data set of time stamps to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#time()} instead.
     */
    @Deprecated
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

    /**
     * Reads a time duration value and its unit from the data set <var>objectPath</var>. It needs to
     * be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, HDF5TimeDuration)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration and its unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDuration readTimeDuration(final String objectPath) throws HDF5JavaException;

    /**
     * Reads a time duration value and its unit from the data set <var>objectPath</var>, converts it
     * to the given <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged
     * as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
     * like
     * 
     * <pre>
     * writer.addTypeVariant(&quot;/dataSetPath&quot;, HDF5TimeUnit.SECONDS.getTypeVariant());
     * </pre>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration and its unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#read(String)} instead.
     */
    @Deprecated
    public HDF5TimeDuration readTimeDurationAndUnit(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads a time duration value from the data set <var>objectPath</var>, converts it to the given
     * <var>timeUnit</var> and returns it as <code>long</code>. It needs to be tagged as one of the
     * type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * This tagging is done by the writer when using
     * {@link IHDF5Writer#writeTimeDuration(String, long, HDF5TimeUnit)} or can be done by calling
     * {@link IHDF5Writer#setTypeVariant(String, HDF5DataTypeVariant)}, most conveniantly by code
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
     * @deprecated Use {@link IHDF5TimeDurationReader#read(String)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDuration)} instead.
     */
    @Deprecated
    public long readTimeDuration(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>. It needs to be tagged as
     * one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time duration in seconds.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     */
    public HDF5TimeDurationArray readTimeDurationArray(final String objectPath)
            throws HDF5JavaException;

    /**
     * Reads a time duration array from the data set <var>objectPath</var>and returns it as a
     * <code>HDF5TimeDuration[]</code>. It needs to be tagged as one of the type variants that
     * indicate a time duration, for example {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The time durations in their respective time unit.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @see #readTimeDurationArray(String, HDF5TimeUnit)
     * @deprecated Use {@link IHDF5TimeDurationReader#readArray(String)} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArray(final String objectPath)
            throws HDF5JavaException;

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
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArray(String)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDurationArray)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArray(final String objectPath, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be
     *         <code>min(size - blockSize*blockNumber,
     *         blockSize)</code>.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray readTimeDurationArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>long[]</code> value in the HDF5 file. It needs to be
     * tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data read from the data set. The length will be
     *         <code>min(size - blockSize*blockNumber,
     *         blockSize)</code>.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlock(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(long[], HDF5TimeUnit)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final HDF5TimeUnit timeUnit) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * It needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public HDF5TimeDurationArray readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>long[]</code> value in the HDF5 file. It needs to be
     * tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @param timeUnit The time unit that the duration should be converted to.
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlockWithOffset(String, int, long)}
     *             and {@link HDF5TimeUnit#convert(HDF5TimeDurationArray)} instead.
     */
    @Deprecated
    public long[] readTimeDurationArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final HDF5TimeUnit timeUnit)
            throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>HDF5TimeDuration[]</code> value in the HDF5 file. It
     * needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The data read from the data set. The length will be
     *         <code>min(size - blockSize*blockNumber,
     *         blockSize)</code>.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlock(String, int, long)} and
     *             {@link HDF5TimeUnit#convert(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlock(final String objectPath,
            final int blockSize, final long blockNumber) throws HDF5JavaException;

    /**
     * Reads a block of a time duration array (of rank 1) from the data set <var>objectPath</var>.
     * The time durations are stored as a <code>HDF5TimeDuration[]</code> value in the HDF5 file. It
     * needs to be tagged as one of the type variants that indicate a time duration, for example
     * {@link HDF5DataTypeVariant#TIME_DURATION_SECONDS}.
     * <p>
     * See {@link #readTimeDuration(String, HDF5TimeUnit)} for how the tagging is done.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The data block read from the data set.
     * @throws HDF5JavaException If the <var>objectPath</var> is not tagged as a type variant that
     *             corresponds to a time duration.
     * @deprecated Use {@link IHDF5TimeDurationReader#readArrayBlockWithOffset(String, int, long)}
     *             and {@link HDF5TimeUnit#convert(HDF5TimeDuration[])} instead.
     */
    @Deprecated
    public HDF5TimeDuration[] readTimeDurationAndUnitArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#duration()} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<HDF5TimeDurationArray>> getTimeDurationArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param timeUnit The time unit that the duration should be converted to.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use {@link IHDF5TimeDurationReader#getArrayNaturalBlocks(String)} and
     *             {@link HDF5TimeUnit#convert(long[], HDF5TimeUnit)} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<long[]>> getTimeDurationArrayNaturalBlocks(
            final String objectPath, final HDF5TimeUnit timeUnit) throws HDF5JavaException;

    /**
     * Provides all natural blocks of this one-dimensional data set of time durations to iterate
     * over.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of a time duration data type or not of rank
     *             1.
     * @deprecated Use {@link IHDF5TimeDurationReader#getArrayNaturalBlocks(String)} instead.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<HDF5TimeDuration[]>> getTimeDurationAndUnitArrayNaturalBlocks(
            final String objectPath) throws HDF5JavaException;

    // *********************
    // Reference
    // *********************

    // //////////////////////////////
    // Specific to object references
    // //////////////////////////////

    /**
     * Resolves the path of a reference which has been read without name resolution.
     * 
     * @param reference Reference encoded as string.
     * @return The path in the HDF5 file.
     * @see #readObjectReferenceArray(String, boolean)
     * @throws HDF5JavaException if <var>reference</var> is not a string-encoded reference.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String resolvePath(final String reference) throws HDF5JavaException;

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads an object reference attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>, resolving the name of the object. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String getObjectReferenceAttribute(final String objectPath, final String attributeName);

    /**
     * Reads an object reference attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @param resolveName If <code>true</code>, resolves the name of the object referenced,
     *            otherwise returns the references itself.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String getObjectReferenceAttribute(final String objectPath, final String attributeName,
            final boolean resolveName);

    /**
     * Reads a 1D object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>, resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] getObjectReferenceArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads a 1D object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] getObjectReferenceArrayAttribute(final String objectPath,
            final String attributeName, final boolean resolveName);

    /**
     * Reads an object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>, resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> getObjectReferenceMDArrayAttribute(final String objectPath,
            final String attributeName);

    /**
     * Reads an object reference array attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The paths of the objects that the references refers to. Each string may be empty, if
     *         the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> getObjectReferenceMDArrayAttribute(final String objectPath,
            final String attributeName, boolean resolveName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads an object reference from the object <var>objectPath</var>, resolving the name of the
     * object. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String readObjectReference(final String objectPath);

    /**
     * Reads an object reference from the object <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param resolveName If <code>true</code>, resolves the name of the object referenced,
     *            otherwise returns the references itself.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String readObjectReference(final String objectPath, final boolean resolveName);

    /**
     * Reads an array of object references from the object <var>objectPath</var>, resolving the
     * names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The array of the paths of objects that the references refers to. Each string may be
     *         empty, if the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArray(final String objectPath);

    /**
     * Reads an array of object references from the object <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The array of the paths of objects that the references refers to. Each string may be
     *         empty, if the corresponding object reference refers to an unnamed object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArray(final String objectPath, boolean resolveName);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>, resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @return The referenced data set paths read from the data set. The length will be min(size -
     *         blockSize*blockNumber, blockSize).
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code> returned
     *            if the data set is long enough).
     * @param blockNumber The number of the block to read (starting with 0, offset: multiply with
     *            <var>blockSize</var>).
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The referenced data set paths read from the data set. The length will be min(size -
     *         blockSize*blockNumber, blockSize).
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArrayBlock(final String objectPath, final int blockSize,
            final long blockNumber, final boolean resolveName);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>, resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset);

    /**
     * Reads a block from an array (of rank 1) of object references from the data set
     * <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockSize The block size (this will be the length of the <code>long[]</code>
     *            returned).
     * @param offset The offset of the block in the data set to start reading from (starting with
     *            0).
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public String[] readObjectReferenceArrayBlockWithOffset(final String objectPath,
            final int blockSize, final long offset, final boolean resolveName);

    /**
     * Reads an array (or rank N) of object references from the object <var>objectPath</var>,
     * resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The multi-dimensional array of the paths of objects that the references refers to.
     *         Each string may be empty, if the corresponding object reference refers to an unnamed
     *         object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArray(final String objectPath);

    /**
     * Reads an array (or rank N) of object references from the object <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The multi-dimensional array of the paths of objects that the references refers to.
     *         Each string may be empty, if the corresponding object reference refers to an unnamed
     *         object.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArray(final String objectPath, boolean resolveName);

    /**
     * Reads a multi-dimensional array of object references from the data set <var>objectPath</var>,
     * resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber);

    /**
     * Reads a multi-dimensional array of object references from the data set <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param blockNumber The block number in each dimension (offset: multiply with the
     *            <var>blockDimensions</var> in the according dimension).
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArrayBlock(final String objectPath,
            final int[] blockDimensions, final long[] blockNumber, final boolean resolveName);

    /**
     * Reads a multi-dimensional array of object references from the data set <var>objectPath</var>,
     * resolving the names of the objects. <br>
     * <i>Note that resolving the name of the object is a time consuming operation. If you don't
     * need the name, but want to dereference the dataset, you don't need to resolve the name if the
     * reader / writer is configured for auto-dereferencing (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}).</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset);

    /**
     * Reads a multi-dimensional array of object references from the data set <var>objectPath</var>. <br>
     * <i>Note: if the reader has been configured to automatically resolve references (see
     * {@link IHDF5ReaderConfigurator#noAutoDereference()}), a reference can be provided in all
     * places where an object path is expected. This is considerably faster than resolving the
     * name/path of the reference if the name/path by itself is not needed.</i>
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param blockDimensions The extent of the block in each dimension.
     * @param offset The offset in the data set to start reading from in each dimension.
     * @param resolveName If <code>true</code>, resolves the names of the objects referenced,
     *            otherwise returns the references itself.
     * @return The referenced data set paths block read from the data set.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public MDArray<String> readObjectReferenceMDArrayBlockWithOffset(final String objectPath,
            final int[] blockDimensions, final long[] offset, final boolean resolveName);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<String[]>> getObjectReferenceArrayNaturalBlocks(
            final String dataSetPath);

    /**
     * Provides all natural blocks of this one-dimensional data set to iterate over.
     * 
     * @see HDF5DataBlock
     * @throws HDF5JavaException If the data set is not of rank 1.
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public Iterable<HDF5DataBlock<String[]>> getObjectReferenceArrayNaturalBlocks(
            final String dataSetPath, final boolean resolveName);

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getObjectReferenceMDArrayNaturalBlocks(
            final String dataSetPath);

    /**
     * Provides all natural blocks of this multi-dimensional data set to iterate over.
     * 
     * @see HDF5MDDataBlock
     * @deprecated Use the corresponding method in {@link IHDF5Reader#reference()}.
     */
    @Deprecated
    public Iterable<HDF5MDDataBlock<MDArray<String>>> getObjectReferenceMDArrayNaturalBlocks(
            final String dataSetPath, final boolean resolveName);
}
