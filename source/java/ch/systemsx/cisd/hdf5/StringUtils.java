/*
 * Copyright 2011 ETH Zuerich, CISD
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

import java.io.UnsupportedEncodingException;

/**
 * Some auxiliary methods for String to Byte conversion. 
 *
 * @author Bernd Rinn
 */
final class StringUtils
{
    private StringUtils()
    {
        // Not to be instantiated.
    }
    
    static byte[] toBytes0TermASCII(String s)
    {
        return toBytes0TermASCII(s, s.length());
    }

    static byte[] toBytes0TermUTF8(String s)
    {
        return toBytes0TermUTF8(s, s.length());
    }

    static byte[] toBytes0TermUTF8(String s, int maxLength)
    {
        return toBytes0Term(s, maxLength, CharacterEncoding.UTF8);
    }
    
    static byte[] toBytes0TermASCII(String s, int maxLength)
    {
        return toBytes0Term(s, maxLength, CharacterEncoding.ASCII);
    }
    
    static byte[] toBytes0Term(String s, int maxLength, CharacterEncoding encoding)
    {
        try
        {
            return (cut(s, maxLength) + '\0').getBytes(encoding.getCharSetName());
        } catch (UnsupportedEncodingException ex)
        {
            return (s + '\0').getBytes();
        }
    }
    
    static String fromBytes0TermUTF8(byte[] data)
    {
        return fromBytes0Term(data, CharacterEncoding.UTF8);
    }

    static String fromBytes0TermASCII(byte[] data)
    {
        return fromBytes0Term(data, CharacterEncoding.ASCII);
    }
    
    static String fromBytes0Term(byte[] data, CharacterEncoding encoding)
    {
        int termIdx;
        for (termIdx = 0; termIdx < data.length && data[termIdx] != 0; ++termIdx)
        {
        }
        try
        {
            return new String(data, 0, termIdx, encoding.getCharSetName());
        } catch (UnsupportedEncodingException ex)
        {
            return new String(data, 0, termIdx);
        }
    }

    static String fromBytes0Term(byte[] data, int startIdx, int maxEndIdx, CharacterEncoding encoding)
    {
        int termIdx;
        for (termIdx = startIdx; termIdx < maxEndIdx && data[termIdx] != 0; ++termIdx)
        {
        }
        try
        {
            return new String(data, startIdx, termIdx - startIdx, encoding.getCharSetName());
        } catch (UnsupportedEncodingException ex)
        {
            return new String(data, startIdx, termIdx - startIdx);
        }
    }

    static String cut(String s, int maxLength)
    {
        if (s.length() > maxLength)
        {
            return s.substring(0, maxLength);
        } else
        {
            return s;
        }
    }

}
