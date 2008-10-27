/*
 * Copyright 2008 ETH Zuerich, CISD
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

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * The abstract base class of Java wrappers for HDF data types.
 * 
 * @author Bernd Rinn
 */
public abstract class HDF5DataType
{

    private final int fileId;

    private final int storageTypeId;
    
    private final int nativeTypeId;

    HDF5DataType(int fileId, int storageTypeId, int nativeTypeId)
    {
        assert fileId >= 0;
        assert storageTypeId >= 0;
        assert nativeTypeId >= 0;

        this.fileId = fileId;
        this.storageTypeId = storageTypeId;
        this.nativeTypeId = nativeTypeId;
    }

    /**
     * Returns the storage data type id of this type.
     */
    int getStorageTypeId()
    {
        return storageTypeId;
    }
    /**
     * Returns the native data type id of this type.
     */
    int getNativeTypeId()
    {
        return nativeTypeId;
    }

    /**
     * Checks whether this type is for file <var>expectedFileId</var>.
     * 
     * @throws HDF5JavaException If this type is not for file <var>expectedFileId</var>.
     */
    void check(final int expectedFileId) throws HDF5JavaException
    {
        if (fileId != expectedFileId)
        {
            throw new HDF5JavaException("Type " + getName() + " is not of this file.");
        }
    }

    /**
     * Returns A name for this type.
     */
    abstract String getName();

    //
    // Object
    //
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + fileId;
        result = prime * result + storageTypeId;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        HDF5DataType other = (HDF5DataType) obj;
        if (fileId != other.fileId)
        {
            return false;
        }
        if (storageTypeId != other.storageTypeId)
        {
            return false;
        }
        return true;
    }

}
