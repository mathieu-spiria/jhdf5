/*
 * Copyright 2009 ETH Zuerich, CISD.
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

import ch.systemsx.cisd.base.mdarray.MDFloatArray;

/**
 * An interface that provides methods for writing <code>float</code> values to HDF5 files.
 * 
 * @author Bernd Rinn
 */
interface IHDF5FloatWriter
{
    // /////////////////////
    // Attributes
    // /////////////////////

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
    public void addFloatAttribute(final String objectPath, final String name, final float value);

    /**
     * Adds a <code>float[]</code> attribute to the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void addFloatArrayAttribute(final String objectPath, final String name,
            final float[] value);

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
     * Creates a <code>float</code> array (of rank 1). Uses a compact storage layout. Should only 
     * be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param length The length of the data set to create.
     */
    public void createFloatArrayCompact(final String objectPath, final long length);

    /**
     * Writes out a <code>float</code> array (of rank 1). Uses a compact storage layout. Should 
     * only be used for small data sets.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeFloatArrayCompact(final String objectPath, final float[] data);

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
     * @param compression The compression parameters of the data set.
     */
    public void writeFloatArray(final String objectPath, final float[] data, 
            final HDF5FloatCompression compression);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float vector to create. When using extendable data sets 
     *          ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data 
     *          set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data 
     *          sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     */
    public void createFloatArray(final String objectPath, final long size, final int blockSize);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. When using extendable data sets 
     *          ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data 
     *          set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data 
     *          sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}) and 
     *                <code>compression</code> is <code>HDF5FloatCompression.NO_COMPRESSION</code>.
     * @param compression The compression parameters of the data set.
     */
    public void createFloatArray(final String objectPath, final long size, final int blockSize,
            final HDF5FloatCompression compression);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have
     * been created by {@link #createFloatArray(String, long, int, HDF5FloatCompression)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createFloatArray(String, long, int, HDF5FloatCompression)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     */
    public void writeFloatArrayBlock(final String objectPath, final float[] data,
            final long blockNumber);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have
     * been created by {@link #createFloatArray(String, long, int, HDF5FloatCompression)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatArrayBlock(String, float[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatArray(String, long, int, HDF5FloatCompression)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     */
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
     * @param compression The compression parameters of the data set.
     */
    public void writeFloatMatrix(final String objectPath, final float[][] data, 
            final HDF5FloatCompression compression);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the float matrix to create.
     * @param sizeY The size of the y dimension of the float matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     */
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
     * @param compression The compression parameters of the data set.
     */
    public void createFloatMatrix(final String objectPath, final long sizeX, final long sizeY,
            final int blockSizeX, final int blockSizeY, final HDF5FloatCompression compression);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumberX The block number in the x dimension (offset: multiply with
     *            <code>data.length</code>).
     * @param blockNumberY The block number in the y dimension (offset: multiply with
     *            <code>data[0.length</code>).
     */
    public void writeFloatMatrixBlock(final String objectPath, final float[][] data,
            final long blockNumberX, final long blockNumberY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     */
    public void writeFloatMatrixBlockWithOffset(final String objectPath, final float[][] data,
            final long offsetX, final long offsetY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} beforehand.
     * <p>
     * Use this method instead of {@link #writeFloatMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createFloatMatrix(String, long, long, int, int, HDF5FloatCompression)} call that was
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
     */
    public void writeFloatMatrixBlockWithOffset(final String objectPath, final float[][] data,
            final int dataSizeX, final int dataSizeY, final long offsetX, final long offsetY);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeFloatMDArray(final String objectPath, final MDFloatArray data);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param compression The compression parameters of the data set.
     */
    public void writeFloatMDArray(final String objectPath, final MDFloatArray data,
            final HDF5FloatCompression compression);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     */
    public void createFloatMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param compression The compression parameters of the data set.
     */
    public void createFloatMDArray(final String objectPath, final long[] dimensions,
            final int[] blockDimensions, final HDF5FloatCompression compression);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     */
    public void writeFloatMDArrayBlock(final String objectPath, final MDFloatArray data,
            final long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set  to start writing to in each dimension.
     */
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
     */
    public void writeFloatMDArrayBlockWithOffset(final String objectPath, final MDFloatArray data,
            final int[] blockDimensions, final long[] offset, final int[] memoryOffset);
}
