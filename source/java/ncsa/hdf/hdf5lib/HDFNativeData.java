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

import ch.systemsx.cisd.base.convert.NativeData;
import ch.systemsx.cisd.base.convert.NativeData.ByteOrder;

/**
 * This class provides a convenience interface to {@link NativeData}.
 */

public class HDFNativeData
{

    static
    {
        NativeData.ensureNativeLibIsLoaded();
    }

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
        return NativeData.shortToByte(new short[] { data }, ByteOrder.NATIVE); 
    }

    /**
     * Converts an <code>int</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] intToByte(int data)
    {
        return NativeData.intToByte(new int[] { data }, ByteOrder.NATIVE); 
    }

    /**
     * Converts a <code>long</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] longToByte(long data)
    {
        return NativeData.longToByte(new long[] { data }, ByteOrder.NATIVE); 
    }

    /**
     * Converts a <code>float</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] floatToByte(float data)
    {
        return NativeData.floatToByte(new float[] { data }, ByteOrder.NATIVE); 
    }

    /**
     * Converts a <code>double</code> value into a <code>byte[]</code>.
     * 
     * @param data The value to convert.
     * @return The array containing the value.
     */
    public static byte[] doubleToByte(double data)
    {
        return NativeData.doubleToByte(new double[] { data }, ByteOrder.NATIVE); 
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
        return NativeData.byteToShort(byteArr, ByteOrder.NATIVE, start, 1)[0];
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
        return NativeData.byteToShort(byteArr, ByteOrder.NATIVE, start, len);
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
        return NativeData.byteToInt(byteArr, ByteOrder.NATIVE, start, 1)[0];
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
        return NativeData.byteToInt(byteArr, ByteOrder.NATIVE, start, len);
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
        return NativeData.byteToLong(byteArr, ByteOrder.NATIVE, start, 1)[0];
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
        return NativeData.byteToLong(byteArr, ByteOrder.NATIVE, start, len);
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
        return NativeData.byteToFloat(byteArr, ByteOrder.NATIVE, start, 1)[0];
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
        return NativeData.byteToFloat(byteArr, ByteOrder.NATIVE, start, len);
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
        return NativeData.byteToDouble(byteArr, ByteOrder.NATIVE, start, 1)[0];
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
        return NativeData.byteToDouble(byteArr, ByteOrder.NATIVE, start, len);
    }

    /**
     * Converts a <code>short[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>short[]</code> array. 
     */
    public static byte[] shortToByte(short[] data)
    {
        return NativeData.shortToByte(data, ByteOrder.NATIVE);
    }

    /**
     * Converts an <code>int[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>int[]</code> array. 
     */
    public static byte[] intToByte(int[] data)
    {
        return NativeData.intToByte(data, ByteOrder.NATIVE);
    }

    /**
     * Converts a <code>long[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>long[]</code> array. 
     */
    public static byte[] longToByte(long[] data)
    {
        return NativeData.longToByte(data, ByteOrder.NATIVE);
    }

    /**
     * Converts a <code>float[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>float[]</code> array. 
     */
    public static byte[] floatToByte(float[] data)
    {
        return NativeData.floatToByte(data, ByteOrder.NATIVE);
    }

    /**
     * Converts a <code>double[]</code> array to a <code>byte[]</code> array.
     * 
     *  @param data The <code>long[]</code> array to convert.
     *  @return The <code>byte[]</code> array that corresponding to the <code>double[]</code> array. 
     */
    public static byte[] doubleToByte(double[] data)
    {
        return NativeData.doubleToByte(data, ByteOrder.NATIVE);
    }

}
