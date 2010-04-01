/*
 * Copyright 2010 ETH Zuerich, CISD
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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;

import ch.systemsx.cisd.base.mdarray.MDAbstractArray;

/**
 * Utilities for working with primitive matrices.
 *
 * @author Bernd Rinn
 */
public class MatrixUtils
{

    static void checkMDArrayDimensions(final Field field, final int[] dimensions,
            final MDAbstractArray<?> array)
    {
        if (Arrays.equals(dimensions, array.dimensions()) == false)
        {
            throw new IllegalArgumentException("The field '" + field.getName()
                    + "' has dimensions " + Arrays.toString(array.dimensions())
                    + " but is supposed to have dimensions " + Arrays.toString(dimensions) + ".");
        }
    }

    static void checkMatrixDimensions(final Field field, final int[] dimensions,
            final Object matrix)
    {
        final int dimX = Array.getLength(matrix);
        final int dimY = Array.getLength(Array.get(matrix, 0));
        if (dimensions.length != 2 || dimensions[0] != dimX || dimensions[1] != dimY)
        {
            throw new IllegalArgumentException("The field '" + field.getName()
                    + "' has dimensions [" + dimX + "," + dimY + "]."
                    + " but is supposed to have dimensions " + Arrays.toString(dimensions) + ".");
        }
    }

    static float[] flatten(float[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final float[] result = new float[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static float[][] shapen(float[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final float[][] result = new float[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

    static double[] flatten(double[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final double[] result = new double[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static double[][] shapen(double[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final double[][] result = new double[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

    static int[] flatten(int[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final int[] result = new int[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static int[][] shapen(int[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final int[][] result = new int[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

    static long[] flatten(long[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final long[] result = new long[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static long[][] shapen(long[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final long[][] result = new long[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

    static short[] flatten(short[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final short[] result = new short[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static short[][] shapen(short[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final short[][] result = new short[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

    static byte[] flatten(byte[][] matrix)
    {
        if (matrix.length == 0)
        {
            throw new IllegalStateException("Matrix must not have a length of 0.");
        }
        final int dimY = matrix.length;
        final int dimX = matrix[0].length;
        for (int i = 1; i < dimY; ++i)
        {
            if (matrix[i].length != dimX)
            {
                throw new IllegalStateException(
                        "All rows in matrix need to have the same number of columns.");
            }
        }
        final byte[] result = new byte[dimX * dimY];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrix[i], 0, result, i * dimX, dimX);
        }
        return result;
    }

    static byte[][] shapen(byte[] matrixData, int[] dims)
    {
        final int dimY = dims[0];
        final int dimX = dims[1];
        final byte[][] result = new byte[dimY][dimX];
        for (int i = 0; i < dimY; ++i)
        {
            System.arraycopy(matrixData, i * dimX, result[i], 0, dimX);
        }
        return result;
    }

}
