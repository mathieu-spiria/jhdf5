/****************************************************************************
 * NCSA HDF                                                                 *
 * National Comptational Science Alliance                                   *
 * University of Illinois at Urbana-Champaign                               *
 * 605 E. Springfield, Champaign IL 61820                                   *
 *                                                                          *
 * For conditions of distribution and use, see the accompanying             *
 * hdf-java/COPYING file.                                                   *
 *                                                                          *
 ****************************************************************************/

package ncsa.hdf.hdf5lib;

/**
 * This class encapsulates native methods to deal with arrays of numbers, converting from numbers to
 * bytes and bytes to numbers.
 * <p>
 * These routines are used by class <b>HDFArray</b> to pass data to and from the HDF-5 library.
 * <p>
 * Methods copyXxxToByte() convert a Java array of primitive numbers (int, short, ...) to a Java
 * array of bytes. Methods copyByteToXxx() convert from a Java array of bytes into a Java array of
 * primitive numbers (int, short, ...)
 * <p>
 * Variant interfaces convert a section of an array, and also can convert to sub-classes of Java
 * <b>Number</b>.
 * <P>
 * <b>See also:</b> ncsa.hdf.hdf5lib.HDFArray.
 */

public class HDFNativeData
{

    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /** Size of a <code>short</code> value in <code>byte</code>. */
    public final static int SHORT_SIZE = 2;

    /** Size of an <code>int</code> value in <code>byte</code>. */
    public final static int INT_SIZE = 4;

    /** Size of a <code>long</code> value in <code>byte</code>. */
    public final static int LONG_SIZE = 8;

    /** Size of a <code>float</code> value in <code>byte</code>. */
    public final static int FLOAT_SIZE = 4;

    /** Size of a <code>double</code> value in <code>byte</code>. */
    public final static int DOUBLE_SIZE = 8;

    /**
     * Copies a range from an array of <code>int</code> into an array of <code>byte</code>.
     * 
     * @param inData The input array of <code>int</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>int</code> to
     *            start
     * @param outData The output array of <code>byte</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>byte</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyIntToByte(int[] inData, int inStart, byte[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>byte</code> into an array of <code>int</code>.
     * 
     * @param inData The input array of <code>byte</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>byte</code> to
     *            start
     * @param outData The output array of <code>int</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>int</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyByteToInt(byte[] inData, int inStart, int[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>long</code> into an array of <code>byte</code>.
     * 
     * @param inData The input array of <code>long</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>long</code> to
     *            start
     * @param outData The output array of <code>byte</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>byte</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyLongToByte(long[] inData, int inStart, byte[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>byte</code> into an array of <code>long</code>.
     * 
     * @param inData The input array of <code>byte</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>byte</code> to
     *            start
     * @param outData The output array of <code>long</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>long</code> to
     *            start
     * @param len The number of <code>long</code> to copy
     */
    public static native void copyByteToLong(byte[] inData, int inStart, long[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>short</code> into an array of <code>byte</code>.
     * 
     * @param inData The input array of <code>short</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>short</code> to
     *            start
     * @param outData The output array of <code>byte</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>byte</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyShortToByte(short[] inData, int inStart, byte[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>byte</code> into an array of <code>short</code>.
     * 
     * @param inData The input array of <code>byte</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>byte</code> to
     *            start
     * @param outData The output array of <code>short</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>short</code> to
     *            start
     * @param len The number of <code>short</code> to copy
     */
    public static native void copyByteToShort(byte[] inData, int inStart, short[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>float</code> into an array of <code>byte</code>.
     * 
     * @param inData The input array of <code>float</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>float</code> to
     *            start
     * @param outData The output array of <code>byte</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>byte</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyFloatToByte(float[] inData, int inStart, byte[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>byte</code> into an array of <code>float</code>.
     * 
     * @param inData The input array of <code>byte</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>byte</code> to
     *            start
     * @param outData The output array of <code>float</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>float</code> to
     *            start
     * @param len The number of <code>float</code> to copy
     */
    public static native void copyByteToFloat(byte[] inData, int inStart, float[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>double</code> into an array of <code>byte</code>.
     * 
     * @param inData The input array of <code>double</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>double</code> to
     *            start
     * @param outData The output array of <code>byte</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>byte</code> to
     *            start
     * @param len The number of <code>int</code> to copy
     */
    public static native void copyDoubleToByte(double[] inData, int inStart, byte[] outData,
            int outStart, int len);

    /**
     * Copies a range from an array of <code>byte</code> into an array of <code>double</code>.
     * 
     * @param inData The input array of <code>byte</code> values.
     * @param inStart The position in the input array <code>inData</code> of <code>byte</code> to
     *            start
     * @param outData The output array of <code>double</code> values.
     * @param outStart The start in the output array <code>byteData</code> of <code>double</code> to
     *            start
     * @param len The number of <code>double</code> to copy
     */
    public static native void copyByteToDouble(byte[] inData, int inStart, double[] outData,
            int outStart, int len);

    /**
     * Converts a <code>byte</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] byteToByte(byte data)
    {
        return new byte[]
            { data };
    }

    /**
     * Converts a <code>short</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] shortToByte(short data)
    {
        final byte[] byteArr = new byte[SHORT_SIZE];
        copyShortToByte(new short[]
            { data }, 0, byteArr, 0, 1);
        return byteArr;
    }

    /**
     * Converts an <code>int</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] intToByte(int data)
    {
        final byte[] byteArr = new byte[INT_SIZE];
        copyIntToByte(new int[]
            { data }, 0, byteArr, 0, 1);
        return byteArr;
    }

    /**
     * Converts a <code>long</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] longToByte(long data)
    {
        final byte[] byteArr = new byte[LONG_SIZE];
        copyLongToByte(new long[]
            { data }, 0, byteArr, 0, 1);
        return byteArr;
    }

    /**
     * Converts a <code>float</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] floatToByte(float data)
    {
        final byte[] byteArr = new byte[FLOAT_SIZE];
        copyFloatToByte(new float[]
            { data }, 0, byteArr, 0, 1);
        return byteArr;
    }

    /**
     * Converts a <code>double</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] doubleToByte(double data)
    {
        final byte[] byteArr = new byte[DOUBLE_SIZE];
        copyDoubleToByte(new double[]
            { data }, 0, byteArr, 0, 1);
        return byteArr;
    }

    /**
     * Converts a range of a <code>byte[]</code> to a <code>short</code> value.
     * 
     * @param byteArr The value to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @return The <code>short</code> value.
     */
    public static short byteToShort(byte[] byteArr, int start)
    {
        final short[] shortContainer = new short[1];
        copyByteToShort(byteArr, start, shortContainer, 0, 1);
        return shortContainer[0];
    }

    /**
     * Converts a <code>byte[]</code> array into a <code>short[]</code> array.
     * 
     * @param byteArr The <code>byte[]</code> to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @param len The number of <code>short</code> values to convert.
     * @return The <code>short[]</code> array.
     */
    public static short[] byteToShort(byte[] byteArr, int start, int len)
    {
        final short[] array = new short[len];
        copyByteToShort(byteArr, start, array, 0, len);
        return array;
    }

    /**
     * Converts a range of a <code>byte[]</code> to a <code>int</code> value.
     * 
     * @param byteArr The value to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @return The <code>int</code> value.
     */
    public static int byteToInt(byte[] byteArr, int start)
    {
        final int[] intContainer = new int[1];
        copyByteToInt(byteArr, start, intContainer, 0, 1);
        return intContainer[0];
    }

    /**
     * Converts a <code>byte[]</code> array into an <code>int[]</code> array.
     * 
     * @param byteArr The <code>byte[]</code> to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @param len The number of <code>int</code> values to convert.
     * @return The <code>int[]</code> array.
     */
    public static int[] byteToInt(byte[] byteArr, int start, int len)
    {
        final int[] array = new int[len];
        copyByteToInt(byteArr, start, array, 0, len);
        return array;
    }

    /**
     * Converts a <code>byte[]</code> array into a <code>long[]</code> array.
     * 
     * @param byteArr The <code>byte[]</code> to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @param len The number of <code>long</code> values to convert.
     * @return The <code>long[]</code> array.
     */
    public static long[] byteToLong(byte[] byteArr, int start, int len)
    {
        final long[] array = new long[len];
        copyByteToLong(byteArr, start, array, 0, len);
        return array;
    }

    /**
     * Converts a <code>byte[]</code> array into a <code>float[]</code> array.
     * 
     * @param byteArr The <code>byte[]</code> to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @param len The number of <code>float</code> values to convert.
     * @return The <code>float[]</code> array.
     */
    public static float[] byteToFloat(byte[] byteArr, int start, int len)
    {
        final float[] array = new float[len];
        copyByteToFloat(byteArr, start, array, 0, len);
        return array;
    }

    /**
     * Converts a <code>byte[]</code> array into a <code>double[]</code> array.
     * 
     * @param byteArr The <code>byte[]</code> to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @param len The number of <code>double</code> values to convert.
     * @return The <code>double[]</code> array.
     */
    public static double[] byteToDouble(byte[] byteArr, int start, int len)
    {
        final double[] array = new double[len];
        copyByteToDouble(byteArr, start, array, 0, len);
        return array;
    }

    /**
     * Converts a range of a <code>byte[]</code> to a <code>long</code> value.
     * 
     * @param byteArr The value to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @return The <code>long</code> value.
     */
    public static long byteToLong(byte[] byteArr, int start)
    {
        final long[] longContainer = new long[1];
        copyByteToLong(byteArr, start, longContainer, 0, 1);
        return longContainer[0];
    }

    /**
     * Converts a range of a <code>byte[]</code> to a <code>float</code> value.
     * 
     * @param byteArr The value to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @return The <code>float</code> value.
     */
    public static float byteToFloat(byte[] byteArr, int start)
    {
        final float[] floatContainer = new float[1];
        copyByteToFloat(byteArr, start, floatContainer, 0, 1);
        return floatContainer[0];
    }

    /**
     * Converts a range of a <code>byte[]</code> to a <code>double</code> value.
     * 
     * @param byteArr The value to convert.
     * @param start The position in the <var>byteArr</var> to start the conversion.
     * @return The <code>double</code> value.
     */
    public static double byteToDouble(byte[] byteArr, int start)
    {
        final double[] doubleContainer = new double[1];
        copyByteToDouble(byteArr, start, doubleContainer, 0, 1);
        return doubleContainer[0];
    }

    /**
     * Converts a <code>short[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>short[]</code> array. 
     */
    public static byte[] shortToByte(short[] data)
    {
        final byte[] byteArr = new byte[SHORT_SIZE * data.length];
        copyShortToByte(data, 0, byteArr, 0, data.length);
        return byteArr;
    }

    /**
     * Converts an <code>int[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>int[]</code> array. 
     */
    public static byte[] intToByte(int[] data)
    {
        final byte[] byteArr = new byte[INT_SIZE * data.length];
        copyIntToByte(data, 0, byteArr, 0, data.length);
        return byteArr;
    }

    /**
     * Converts a <code>long[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>long[]</code> array. 
     */
    public static byte[] longToByte(long[] data)
    {
        final byte[] byteArr = new byte[LONG_SIZE * data.length];
        copyLongToByte(data, 0, byteArr, 0, data.length);
        return byteArr;
    }

    /**
     * Converts a <code>float[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>float[]</code> array. 
     */
    public static byte[] floatToByte(float[] data)
    {
        final byte[] byteArr = new byte[FLOAT_SIZE * data.length];
        copyFloatToByte(data, 0, byteArr, 0, data.length);
        return byteArr;
    }

    /**
     * Converts a <code>double[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>double[]</code> array. 
     */
    public static byte[] doubleToByte(double[] data)
    {
        final byte[] byteArr = new byte[DOUBLE_SIZE * data.length];
        copyDoubleToByte(data, 0, byteArr, 0, data.length);
        return byteArr;
    }

}
