/*
 * Copyright 2007 ETH Zuerich, CISD
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

import static org.testng.AssertJUnit.assertEquals;

import java.util.BitSet;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ch.rinn.restrictions.Friend;

/**
 * Test cases for the BitSet conversion from / to storage form.
 * 
 * @author Bernd Rinn
 */
@Friend(toClasses = BitSetConversionUtils.class)
public class BitSetConversionTest
{
    private BitSet create(final Integer... indices)
    {
        final BitSet bs = new BitSet();
        for (final int index : indices)
        {
            bs.set(index);
        }
        return bs;
    }

    @DataProvider
    public Object[][] createBitSets()
    {
        final BitSet full4w = new BitSet();
        full4w.set(0, 256);

        return new Object[][]
            {
                { create() },
                { create(0) },
                { create(31) },
                { create(64) },
                { create(128) },
                { create(63, 191) },
                { create(64, 192) },
                { create(17, 88, 155) },
                { full4w }, };
    }

    @Test(dataProvider = "createBitSets")
    public void testBitSetRoundTripGeneric(final BitSet bs)
    {
        final long[] bsArray = BitSetConversionUtils.toStorageFormGeneric(bs);
        final BitSet bs2 = BitSetConversionUtils.fromStorageFormGeneric(bsArray);
        assertEquals(bs, bs2);
    }

    @Test(dataProvider = "createBitSets")
    public void testBitSetRoundTrip(final BitSet bs)
    {
        final long[] bsArray = BitSetConversionUtils.toStorageForm(bs);
        final BitSet bs2 = BitSetConversionUtils.fromStorageForm(bsArray);
        assertEquals(bs, bs2);
    }

}
