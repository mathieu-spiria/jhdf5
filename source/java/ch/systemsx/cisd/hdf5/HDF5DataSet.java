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

import static ch.systemsx.cisd.hdf5.hdf5lib.H5D.H5Dclose;
import static ch.systemsx.cisd.hdf5.hdf5lib.H5S.H5Sclose;

/**
 * An object to represent an HDF5 data set.
 * 
 * @author Bernd Rinn
 */
public class HDF5DataSet implements AutoCloseable
{
    private final String datasetPath;

    private final HDF5StorageLayout layout;

    private final int dataspaceId;

    private final long[] dimensions;

    private int datasetId;

    HDF5DataSet(String datasetPath, int datasetId, int dataspaceId, long[] dimensions,
            HDF5StorageLayout layout)
    {
        this.datasetPath = datasetPath;
        this.datasetId = datasetId;
        this.dataspaceId = dataspaceId;
        this.dimensions = dimensions;
        this.layout = layout;
    }

    /**
     * Returns the path of this data set.
     */
    public String getDatasetPath()
    {
        return datasetPath;
    }

    int getDatasetId()
    {
        return datasetId;
    }

    int getDataspaceId()
    {
        return dataspaceId;
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
        if (datasetId > 0)
        {
            H5Sclose(dataspaceId);
            H5Dclose(datasetId);
            datasetId = -1;
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((datasetPath == null) ? 0 : datasetPath.hashCode());
        result = prime * result + datasetId;
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
        HDF5DataSet other = (HDF5DataSet) obj;
        if (datasetPath == null)
        {
            if (other.datasetPath != null)
            {
                return false;
            }
        } else if (!datasetPath.equals(other.datasetPath))
        {
            return false;
        }
        if (datasetId != other.datasetId)
        {
            return false;
        }
        return true;
    }

}
