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

package ncsa.hdf.hdf5lib;

import java.util.Arrays;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * Test cases for {@link HDFNativeData}.
 * 
 * @author Bernd Rinn
 */
public class HDFNativeDataTest
{
    @Test
    public void testByteToByte()
    {
        final byte b = 127;
        final byte[] barr = HDFNativeData.byteToByte(b);
        assertEquals(1, barr.length);
        assertEquals(b, barr[0]);
    }

    @SuppressWarnings("unused")
    @DataProvider(name = "getOfs")
    private Object[][] getOfs()
    {
        return new Object[][]
            {
                { 0, 0 },
                { 0, 1 },
                { 0, 2 },
                { 0, 3 }, 
                { 1, 0 },
                { 1, 1 },
                { 1, 2 },
                { 1, 3 }, 
                { 2, 0 },
                { 2, 1 },
                { 2, 2 },
                { 2, 3 }, 
                { 3, 0 },
                { 3, 1 },
                { 3, 2 },
                { 3, 3 }, 
            };
    }

    @Test(dataProvider = "getOfs")
    public void testIntToByteToInt(int sourceOfs, int targetOfs)
    {
        final int sizeOfTarget = 4;
        final int[] orignalArr = new int[]
                                   { -1, 17, 100000, -1000000 };
        final int[] iarr = new int[sourceOfs + orignalArr.length];
        System.arraycopy(orignalArr, 0, iarr, sourceOfs, orignalArr.length);
        final byte[] barr = new byte[iarr.length * sizeOfTarget + targetOfs];
        HDFNativeData.copyIntToByte(iarr, sourceOfs, barr, targetOfs, orignalArr.length);
        final int[] iarr2 = new int[(barr.length - targetOfs) / sizeOfTarget];
        HDFNativeData.copyByteToInt(barr, targetOfs, iarr2, sourceOfs, orignalArr.length);
        assertTrue(Arrays.equals(iarr, iarr2));
    }

    @Test(dataProvider = "getOfs")
    public void testLongToByteToLong(int sourceOfs, int targetOfs)
    {
        final int sizeOfTarget = 8;
        final long[] orignalArr = new long[]
                                   { -1, 17, 100000, -1000000 };
        final long[] iarr = new long[sourceOfs + orignalArr.length];
        System.arraycopy(orignalArr, 0, iarr, sourceOfs, orignalArr.length);
        final byte[] barr = new byte[iarr.length * sizeOfTarget + targetOfs];
        HDFNativeData.copyLongToByte(iarr, sourceOfs, barr, targetOfs, orignalArr.length);
        final long[] iarr2 = new long[(barr.length - targetOfs) / sizeOfTarget];
        HDFNativeData.copyByteToLong(barr, targetOfs, iarr2, sourceOfs, orignalArr.length);
        assertTrue(Arrays.equals(iarr, iarr2));
    }

    @Test(dataProvider = "getOfs")
    public void testShortToByteToShort(int sourceOfs, int targetOfs)
    {
        final int sizeOfTarget = 8;
        final short[] orignalArr = new short[]
                                   { -1, 17, 20000, (short) -50000 };
        final short[] iarr = new short[sourceOfs + orignalArr.length];
        System.arraycopy(orignalArr, 0, iarr, sourceOfs, orignalArr.length);
        final byte[] barr = new byte[iarr.length * sizeOfTarget + targetOfs];
        HDFNativeData.copyShortToByte(iarr, sourceOfs, barr, targetOfs, orignalArr.length);
        final short[] iarr2 = new short[(barr.length - targetOfs) / sizeOfTarget];
        HDFNativeData.copyByteToShort(barr, targetOfs, iarr2, sourceOfs, orignalArr.length);
        assertTrue(Arrays.equals(iarr, iarr2));
    }

    @Test(dataProvider = "getOfs")
    public void testFloatToByteToFloat(int sourceOfs, int targetOfs)
    {
        final int sizeOfTarget = 8;
        final float[] orignalArr = new float[]
                                   { -1, 17, 3.14159f, -1e6f };
        final float[] iarr = new float[sourceOfs + orignalArr.length];
        System.arraycopy(orignalArr, 0, iarr, sourceOfs, orignalArr.length);
        final byte[] barr = new byte[iarr.length * sizeOfTarget + targetOfs];
        HDFNativeData.copyFloatToByte(iarr, sourceOfs, barr, targetOfs, orignalArr.length);
        final float[] iarr2 = new float[(barr.length - targetOfs) / sizeOfTarget];
        HDFNativeData.copyByteToFloat(barr, targetOfs, iarr2, sourceOfs, orignalArr.length);
        assertTrue(Arrays.equals(iarr, iarr2));
    }

    @Test(dataProvider = "getOfs")
    public void testDoubleToByteToDouble(int sourceOfs, int targetOfs)
    {
        final int sizeOfTarget = 8;
        final double[] orignalArr = new double[]
                                   { -1, 17, 3.14159, -1e42 };
        final double[] iarr = new double[sourceOfs + orignalArr.length];
        System.arraycopy(orignalArr, 0, iarr, sourceOfs, orignalArr.length);
        final byte[] barr = new byte[iarr.length * sizeOfTarget + targetOfs];
        HDFNativeData.copyDoubleToByte(iarr, sourceOfs, barr, targetOfs, orignalArr.length);
        final double[] iarr2 = new double[(barr.length - targetOfs) / sizeOfTarget];
        HDFNativeData.copyByteToDouble(barr, targetOfs, iarr2, sourceOfs, orignalArr.length);
        assertTrue(Arrays.equals(iarr, iarr2));
    }
}
