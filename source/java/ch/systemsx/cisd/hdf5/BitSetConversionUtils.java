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

import java.lang.reflect.Field;
import java.util.BitSet;

import org.apache.commons.lang.SystemUtils;

import ch.rinn.restrictions.Private;

/**
 * Methods for converting {@link BitSet}s to a storage form suitable for storing in an HDF5 file.
 * 
 * @author Bernd Rinn
 */
class BitSetConversionUtils
{

    private final static int ADDRESS_BITS_PER_WORD = 6;

    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

    private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;

    private static final Field BIT_SET_WORDS = getBitSetWords();

    private static final Field BIT_SET_WORDS_IN_USE = getBitSetWordsInUse();

    private static Field getBitSetWords()
    {
        try
        {
            final Field bitsField =
                    BitSet.class.getDeclaredField(SystemUtils.IS_JAVA_1_5 ? "bits" : "words");
            bitsField.setAccessible(true);
            return bitsField;
        } catch (final NoSuchFieldException ex)
        {
            return null;
        }
    }

    private static Field getBitSetWordsInUse()
    {
        try
        {
            final Field unitsInUseField =
                    BitSet.class.getDeclaredField(SystemUtils.IS_JAVA_1_5 ? "unitsInUse"
                            : "wordsInUse");
            unitsInUseField.setAccessible(true);
            return unitsInUseField;
        } catch (final NoSuchFieldException ex)
        {
            return null;
        }
    }

    static BitSet fromStorageForm(final long[] serializedWordArray)
    {
        if (BIT_SET_WORDS != null)
        {
            return fromStorageFormFast(serializedWordArray);
        } else
        {
            return fromStorageFormGeneric(serializedWordArray);
        }
    }

    private static BitSet fromStorageFormFast(final long[] serializedWordArray)
    {
        try
        {
            final BitSet result = new BitSet();
            BIT_SET_WORDS.set(result, serializedWordArray);
            // This is a Java 1.6 "speciality"
            if (serializedWordArray[serializedWordArray.length - 1] == 0)
            {
                BIT_SET_WORDS_IN_USE.set(result, serializedWordArray.length - 1);
            } else
            {
                BIT_SET_WORDS_IN_USE.set(result, serializedWordArray.length);
            }
            return result;
        } catch (final IllegalAccessException ex)
        {
            throw new IllegalAccessError(ex.getMessage());
        }
    }

    @Private
    static BitSet fromStorageFormGeneric(final long[] serializedWordArray)
    {
        final BitSet result = new BitSet();
        for (int wordIndex = 0; wordIndex < serializedWordArray.length; ++wordIndex)
        {
            final long word = serializedWordArray[wordIndex];
            for (int bitInWord = 0; bitInWord < BITS_PER_WORD; ++bitInWord)
            {
                if ((word & 1L << bitInWord) != 0)
                {
                    result.set(wordIndex << ADDRESS_BITS_PER_WORD | bitInWord);
                }
            }
        }
        return result;
    }

    static long[] toStorageForm(final BitSet data)
    {
        if (BIT_SET_WORDS != null)
        {
            return toStorageFormFast(data);
        } else
        {
            return toStorageFormGeneric(data);
        }
    }

    private static long[] toStorageFormFast(final BitSet data)
    {
        try
        {
            return (long[]) BIT_SET_WORDS.get(data);
        } catch (final IllegalAccessException ex)
        {
            throw new IllegalAccessError(ex.getMessage());
        }
    }

    private static int wordIndex(final int bitIndex)
    {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    private static long bitInIndex(final int bitIndex)
    {
        return 1L << (bitIndex & BIT_INDEX_MASK);
    }

    // @Private
    static long[] toStorageFormGeneric(final BitSet data)
    {
        final long[] words = new long[data.size() >> ADDRESS_BITS_PER_WORD];
        for (int bitIndex = data.nextSetBit(0); bitIndex >= 0; bitIndex =
                data.nextSetBit(bitIndex + 1))
        {
            final int wordIndex = wordIndex(bitIndex);
            words[wordIndex] |= bitInIndex(bitIndex);
        }
        return words;
    }

}
