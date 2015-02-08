/*
 * Copyright 2015 ETH Zuerich, SIS
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

import static ch.systemsx.cisd.hdf5.hdf5lib.H5P.H5Pclose;
import static ch.systemsx.cisd.hdf5.hdf5lib.H5S.H5Sclose;

/**
 * An object to represent a template of an HDF5 data set.
 * <p>
 * <i>Close it after usage is finished as otherwise you will leak resources from the HDF5
 * library.</i>
 * 
 * @author Bernd Rinn
 */
public class HDF5DataSetTemplate implements AutoCloseable
{
    private final HDF5StorageLayout layout;

    private final long[] dimensions;

    private final int dataSetCreationPropertyListId;

    private final boolean closeCreationPropertyListId;

    private final int storageDataTypeId;

    private int dataspaceId;

    HDF5DataSetTemplate(int dataspaceId, int dataSetCreationPropertyListId,
            boolean closeCreationPropertyListId, int storageDataTypeId, long[] dimensions,
            HDF5StorageLayout layout)
    {
        this.dataspaceId = dataspaceId;
        this.dataSetCreationPropertyListId = dataSetCreationPropertyListId;
        this.closeCreationPropertyListId = closeCreationPropertyListId;
        this.storageDataTypeId = storageDataTypeId;
        this.dimensions = dimensions;
        this.layout = layout;
    }

    int getDataspaceId()
    {
        return dataspaceId;
    }

    int getDataSetCreationPropertyListId()
    {
        return dataSetCreationPropertyListId;
    }

    int getStorageDataTypeId()
    {
        return storageDataTypeId;
    }

    long[] getDimensions()
    {
        return dimensions;
    }

    HDF5StorageLayout getLayout()
    {
        return layout;
    }

    @Override
    public void close()
    {
        if (dataspaceId > 0)
        {
            H5Sclose(dataspaceId);
            dataspaceId = -1;
            if (closeCreationPropertyListId)
            {
                H5Pclose(dataSetCreationPropertyListId);
            }
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + dataspaceId;
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
        HDF5DataSetTemplate other = (HDF5DataSetTemplate) obj;
        if (dataspaceId != other.dataspaceId)
        {
            return false;
        }
        return true;
    }

}
