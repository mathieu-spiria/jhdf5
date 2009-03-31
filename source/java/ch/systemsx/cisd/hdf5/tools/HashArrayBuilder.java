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

package ch.systemsx.cisd.hdf5.tools;

/**
 * Equivalent to the {@link StringBuilder}, but for hashes which are <code>byte</code> arrays.
 *
 * @author Bernd Rinn
 */
final class HashArrayBuilder
{
    private final byte[] buf;
    
    private int offset;
    
    HashArrayBuilder(int bufLen)
    {
        buf = new byte[bufLen];
    }
    
    void append(byte[] hashOrNull)
    {
        if (hashOrNull == null)
        {
            return;
        }
        System.arraycopy(hashOrNull, 0, buf, offset, hashOrNull.length);
        offset += hashOrNull.length;
    }
    
    byte[] getHashes()
    {
        return buf;
    }
}
