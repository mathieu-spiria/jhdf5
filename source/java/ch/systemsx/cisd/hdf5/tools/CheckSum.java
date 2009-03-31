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
 * An enum representing the checksum to compute when archiving new files.
 * 
 * @author Bernd Rinn
 */
public enum CheckSum
{
    NONE(null, 0), MD5("MD5", 16), SHA1("SHA-1", 20);

    private final String algorithmNameOrNull;

    private final int hashLength;

    CheckSum(String algorithmName, int hashLength)
    {
        this.algorithmNameOrNull = algorithmName;
        this.hashLength = hashLength;
    }

    public String tryGetAlgorithm()
    {
        return algorithmNameOrNull;
    }

    /**
     * Returns the size of hashes of this checksum.
     */
    public int getHashLength()
    {
        return hashLength;
    }

    /**
     * Returns <code>true</code>, if the buffer <var>hashOrNull</var> has the correct length for
     * this checksum.
     */
    public boolean hasCorrectLength(byte[] hashOrNull)
    {
        return hashLength == ((hashOrNull == null) ? 0 : hashOrNull.length);
    }

    /**
     * Returns a new hash buffer big enough for a hash of this checksum.
     */
    public byte[] createHashBuffer()
    {
        return new byte[hashLength];
    }
}
