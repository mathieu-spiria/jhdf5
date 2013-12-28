/*
 * Copyright 2007 - 2014 ETH Zuerich, CISD and SIS.
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
public interface IHDF5FloatWriter extends IHDF5FloatReader
{
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
     */
    public void setAttr(String objectPath, String name, float value);

    /**
     * Set a <code>float[]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void setArrayAttr(String objectPath, String name, float[] value);

    /**
     * Set a multi-dimensional code>float</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void setMDArrayAttr(String objectPath, String name, MDFloatArray value);

    /**
     * Set a <code>float[][]</code> attribute on the referenced object.
     * <p>
     * The referenced object must exist, that is it need to have been written before by one of the
     * <code>write()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param value The value of the attribute.
     */
    public void setMatrixAttr(String objectPath, String name, float[][] value);
    
    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes out a <code>float</code> value.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param value The value to write.
     */
    public void write(String objectPath, float value);

    /**
     * Writes out a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     */
    public void writeArray(String objectPath, float[] data);

    /**
     * Writes out a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param features The storage features of the data set.
     */
    public void writeArray(String objectPath, float[] data, 
            HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size When the writer is configured to use extendable data types (see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}), the initial size
     *            and the chunk size of the array will be <var>size</var>. When the writer is
     *            configured to <i>enforce</i> a on-extendable data set, the initial size equals the
     *            total size and will be <var>size</var>.
     */
    public void createArray(String objectPath, int size);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. When using extendable data sets 
     *          ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data 
     *          set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data 
     *          sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}).
     */
    public void createArray(String objectPath, long size, int blockSize);

    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the <code>float</code> array to create. When <i>requesting</i> a 
     *            chunked data set (e.g. {@link HDF5FloatStorageFeatures#FLOAT_CHUNKED}), 
     *            the initial size of the array will be 0 and the chunk size will be <var>arraySize</var>. 
     *            When <i>allowing</i> a chunked data set (e.g. 
     *            {@link HDF5FloatStorageFeatures#FLOAT_NO_COMPRESSION} when the writer is 
     *            not configured to avoid extendable data types, see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}), the initial size
     *            and the chunk size of the array will be <var>arraySize</var>. When <i>enforcing</i> a 
     *            on-extendable data set (e.g. 
     *            {@link HDF5FloatStorageFeatures#FLOAT_CONTIGUOUS}), the initial size equals 
     *            the total size and will be <var>arraySize</var>.
     * @param features The storage features of the data set.
     */
    public void createArray(String objectPath, int size,
            HDF5FloatStorageFeatures features);
    
    /**
     * Creates a <code>float</code> array (of rank 1).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param size The size of the float array to create. When using extendable data sets 
     *          ((see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()})), then no data 
     *          set smaller than this size can be created, however data sets may be larger.
     * @param blockSize The size of one block (for block-wise IO). Ignored if no extendable data 
     *          sets are used (see {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}) and 
     *                <code>features</code> is <code>HDF5FloatStorageFeature.FLOAT_NO_COMPRESSION</code>.
     * @param features The storage features of the data set.
     */
    public void createArray(String objectPath, long size, int blockSize,
            HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have
     * been created by {@link #createArray(String, long, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * <i>Note:</i> For best performance, the block size in this method should be chosen to be equal
     * to the <var>blockSize</var> argument of the
     * {@link #createArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param blockNumber The number of the block to write.
     */
    public void writeArrayBlock(String objectPath, float[] data,
            long blockNumber);

    /**
     * Writes out a block of a <code>float</code> array (of rank 1). The data set needs to have
     * been created by {@link #createArray(String, long, int, HDF5FloatStorageFeatures)}
     * beforehand.
     * <p>
     * Use this method instead of {@link #writeArrayBlock(String, float[], long)} if the
     * total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createArray(String, long, int, HDF5FloatStorageFeatures)} call that was used to
     * create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. The length defines the block size. Must not be
     *            <code>null</code> or of length 0.
     * @param dataSize The (real) size of <code>data</code> (needs to be <code><= data.length</code>
     *            )
     * @param offset The offset in the data set to start writing to.
     */
    public void writeArrayBlockWithOffset(String objectPath, float[] data,
            int dataSize, long offset);

    /**
     * Writes out a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeMatrix(String objectPath, float[][] data);

    /**
     * Writes out a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     */
    public void writeMatrix(String objectPath, float[][] data, 
            HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of one block in the x dimension. See
     *            {@link #createMDArray(String, int[])} on the different
     *            meanings of this parameter.
     * @param sizeY The size of one block in the y dimension. See
     *            {@link #createMDArray(String, int[])} on the different
     *            meanings of this parameter.
     */
    public void createMatrix(String objectPath, int sizeX, int sizeY);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of one block in the x dimension. See
     *            {@link #createMDArray(String, int[], HDF5FloatStorageFeatures)} on the different
     *            meanings of this parameter.
     * @param sizeY The size of one block in the y dimension. See
     *            {@link #createMDArray(String, int[], HDF5FloatStorageFeatures)} on the different
     *            meanings of this parameter.
     * @param features The storage features of the data set.
     */
    public void createMatrix(String objectPath, int sizeX, int sizeY,
    		HDF5FloatStorageFeatures features);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the float matrix to create.
     * @param sizeY The size of the y dimension of the float matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     */
    public void createMatrix(String objectPath, long sizeX, long sizeY,
            int blockSizeX, int blockSizeY);

    /**
     * Creates a <code>float</code> matrix (array of rank 2).
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param sizeX The size of the x dimension of the float matrix to create.
     * @param sizeY The size of the y dimension of the float matrix to create.
     * @param blockSizeX The size of one block in the x dimension.
     * @param blockSizeY The size of one block in the y dimension.
     * @param features The storage features of the data set.
     */
    public void createMatrix(String objectPath, long sizeX, long sizeY,
            int blockSizeX, int blockSizeY, HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} if the total
     * size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the size of <var>data</var> in this method should match
     * the <var>blockSizeX/Y</var> arguments of the
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that was
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
    public void writeMatrixBlock(String objectPath, float[][] data,
            long blockNumberX, long blockNumberY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that was
     * used to create the data set.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write.
     * @param offsetX The x offset in the data set to start writing to.
     * @param offsetY The y offset in the data set to start writing to.
     */
    public void writeMatrixBlockWithOffset(String objectPath, float[][] data,
            long offsetX, long offsetY);

    /**
     * Writes out a block of a <code>float</code> matrix (array of rank 2). The data set needs to
     * have been created by
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} beforehand.
     * <p>
     * Use this method instead of {@link #writeMatrixBlock(String, float[][], long, long)} if
     * the total size of the data set is not a multiple of the block size.
     * <p>
     * <i>Note:</i> For best performance, the typical <var>dataSize</var> in this method should be
     * chosen to be equal to the <var>blockSize</var> argument of the
     * {@link #createMatrix(String, long, long, int, int, HDF5FloatStorageFeatures)} call that was
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
    public void writeMatrixBlockWithOffset(String objectPath, float[][] data,
            int dataSizeX, int dataSizeY, long offsetX, long offsetY);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     */
    public void writeMDArray(String objectPath, MDFloatArray data);

    /**
     * Writes out a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param features The storage features of the data set.
     */
    public void writeMDArray(String objectPath, MDFloatArray data,
            HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions When the writer is configured to use extendable data types (see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}), the initial dimensions
     *            and the dimensions of a chunk of the array will be <var>dimensions</var>. When the 
     *            writer is configured to <i>enforce</i> a on-extendable data set, the initial dimensions 
     *            equal the dimensions and will be <var>dimensions</var>.
     */
    public void createMDArray(String objectPath, int[] dimensions);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     */
    public void createMDArray(String objectPath, long[] dimensions,
            int[] blockDimensions);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the <code>float</code> array to create. When <i>requesting</i> 
     *            a chunked data set (e.g. {@link HDF5FloatStorageFeatures#FLOAT_CHUNKED}), 
     *            the initial size of the array will be 0 and the chunk size will be <var>dimensions</var>. 
     *            When <i>allowing</i> a chunked data set (e.g. 
     *            {@link HDF5FloatStorageFeatures#FLOAT_NO_COMPRESSION} when the writer is 
     *            not configured to avoid extendable data types, see
     *            {@link IHDF5WriterConfigurator#dontUseExtendableDataTypes()}), the initial size
     *            and the chunk size of the array will be <var>dimensions</var>. When <i>enforcing</i> a 
     *            on-extendable data set (e.g. 
     *            {@link HDF5FloatStorageFeatures#FLOAT_CONTIGUOUS}), the initial size equals 
     *            the total size and will be <var>dimensions</var>.
     * @param features The storage features of the data set.
     */
    public void createMDArray(String objectPath, int[] dimensions,
            HDF5FloatStorageFeatures features);

    /**
     * Creates a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param dimensions The dimensions of the array.
     * @param blockDimensions The dimensions of one block (chunk) of the array.
     * @param features The storage features of the data set.
     */
    public void createMDArray(String objectPath, long[] dimensions,
            int[] blockDimensions, HDF5FloatStorageFeatures features);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param blockNumber The block number in each dimension (offset: multiply with the extend in
     *            the according dimension).
     */
    public void writeMDArrayBlock(String objectPath, MDFloatArray data,
            long[] blockNumber);

    /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>. All columns need to have the
     *            same length.
     * @param offset The offset in the data set  to start writing to in each dimension.
     */
    public void writeMDArrayBlockWithOffset(String objectPath, MDFloatArray data,
            long[] offset);

   /**
     * Writes out a block of a multi-dimensional <code>float</code> array.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param data The data to write. Must not be <code>null</code>.
     * @param blockDimensions The dimensions of the block to write to the data set.
     * @param offset The offset of the block in the data set to start writing to in each dimension.
     * @param memoryOffset The offset of the block in the <var>data</var> array.
     */
    public void writeMDArrayBlockWithOffset(String objectPath, MDFloatArray data,
            int[] blockDimensions, long[] offset, int[] memoryOffset);
}
